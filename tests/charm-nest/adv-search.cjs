// Adversarial checks (task G) of the order search and the Orders › Cancelled tab. Each block proved a bug before its fix:
//  1 · a restored order still read "Cancelled" in the search (its stale copy of the Cancelled list)
//  2 · a cloud lookup whose reads all failed said "not in the cloud" and kept that answer for two minutes
//  3 · a pasted "#4176 208 841" finds the order; a pasted run of digits longer than any Etsy number asks the cloud nothing
//  4 · Ctrl+K with a window over the order window closed the order window under it and opened the search behind it
//  5 · a Restore racing a read already in flight (the orders check's ids, the tab's list, AutoCancel's newest records)
//      brought the order back into Cancelled: counted again, flown in as a new cancel, and kept out of the next pull
//  6 · the Cancelled tab searched for an older order whose lookup failed spun "Reading…" for good
// Headless Chromium against the local fake site (bridge-server.cjs); every request off the loopback is aborted; the cancel
// records and the cloud's answers are this file's own (page.route). No Etsy call.
//   node tests/charm-nest/adv-search.cjs [playwright-core dir]
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.now() / 1000) + 3 * DAY, NOW = Date.now(), H = 3600000;
const order = (rid, buyer, sku, ago) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - ago * DAY, updateTs: SHIP - ago * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: String(rid) + '1', listingId: '1800000001', sku, title: sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
const ORDERS = [order(4176208841, 'Hannah Whitford', 'BLOOMING_20239', 2), order(4100000002, 'Grace Hopper', 'SS-STAR-07', 3), order(4100000020, 'Ada Byron', 'GF-MOON-1', 4)];
const REC = (orderId, by, ago, extra) => Object.assign({ orderId, by, why: 'test', at: NOW - ago * H, buyer: 'B ' + orderId, sheets: [], lines: [{ transactionId: '1', sku: 'SKU-' + orderId, title: 't', quantity: 1 }], source: by === 'Etsy' ? 'etsy' : 'sorter' }, extra || {});
const FAIL = '4170000777', OLD = '4099999001';

async function main() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await ctx.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await ctx.newPage(), errors = [];
    page.setDefaultTimeout(15000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    // the cancel records (a person's for Grace's order, which is also in the pull; one older than the list the tab reads)
    let records = [REC('4100000002', 'Anna', 1), REC('4100000003', 'Etsy', 2), REC(OLD, 'Ben', 900)];
    const asked = [], holds = {};
    const gate = name => { let open; const p = new Promise(r => { open = r; }); holds[name] = { p, open, seen: false }; };
    await page.route(/\/\.netlify\/functions\/(charmNestLibrary|designArchive)/, async r => {
      const req = r.request(), url = new URL(req.url()); let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {}
      const op = b.op || url.searchParams.get('op'), id = String(b.orderId || b.q || url.searchParams.get('id') || '');
      const ok = body => r.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) }).catch(() => {});
      asked.push({ op, id, idsOnly: !!b.idsOnly, limit: b.limit });
      if (['timelineGet', 'poolList', 'findSheets', 'get'].includes(op) && id.length > 13) return ok({});
      if (id === FAIL && ['timelineGet', 'poolList', 'findSheets', 'get'].includes(op)) return r.fulfill({ status: 500, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: '{"error":"boom"}' });
      if (op === 'cancelList') {
        const snap = records.slice(), key = b.idsOnly ? 'ids' : b.limit === 50 ? 'poll' : 'list', g = holds[key];
        if (g) { g.seen = true; await g.p; }
        if (b.idsOnly) return ok({ ids: snap.map(c => c.orderId), truncated: false });
        const n = Math.max(1, Math.min(500, Math.round(+b.limit) || 200)), s = snap.filter(c => c.orderId !== OLD).sort((a, c) => c.at - a.at);   // (OLD: beyond the pages read)
        return ok({ list: s.slice(0, n), truncated: false });
      }
      if (op === 'cancelCheck') return r.fulfill({ status: 503, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: '{"error":"unavailable"}' });
      if (op === 'cancelRestore') { records = records.filter(c => c.orderId !== String(b.orderId)); return ok({ ok: true }); }
      return r.fallback();
    });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderSearch && window.Cancelled && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: order.createTs * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll();
      await Cancelled.load(true); await Cancelled.history();
    }, ORDERS);
    const etsyCalls = () => srv.st.calls.filter(c => /etsy|listOpenOrders|Receipt/i.test(c.name)).length, etsy0 = etsyCalls();

    let bad = 0; const step = async (name, fn) => { try { await fn(); } catch (e) { if (!process.env.SOFT) throw e; bad++; console.log('  ✗ ' + name + ': ' + String(e.message).split('\n')[0]); } };
    await step('1', async () => {
    /* 1 · restored: the search no longer reads it as cancelled */
    await page.keyboard.press('/');
    await page.waitForFunction(() => OrderSearch.isOpen());
    await page.waitForFunction(() => { const f = OrderSearch.find('4100000002'); return f && f.entry.cancel && f.state.cancelled; }, null, { timeout: 8000 });
    await page.keyboard.press('Escape'); await page.waitForFunction(() => document.getElementById('cnSearch').hidden);
    await page.evaluate(() => Cancelled.restore('4100000002'));
    const after = await page.evaluate(() => { const f = OrderSearch.find('4100000002'); return { cancelled: f.state.cancelled, pill: f.state.pill }; });
    assert.equal(after.cancelled, false, 'a restored order no longer reads Cancelled in the search: ' + JSON.stringify(after));
    console.log('  ✓ 1 restored order: the search stops calling it cancelled');

    });
    await step('3', async () => {
    /* 3 · pasted with spaces and a #; a paste longer than any Etsy number asks the cloud nothing */
    await page.keyboard.press('/'); await page.waitForFunction(() => OrderSearch.isOpen());
    await page.fill('#cnsQ', '#4176 208 841');
    assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('#cnsList .cnsCard')].map(n => n.dataset.rid)), ['4176208841'], 'a pasted "#4176 208 841" finds its order');
    const long = '4176208841 4100000020 4100000002 4176208841';
    await page.fill('#cnsQ', long); await page.waitForTimeout(700);
    await page.fill('#cnsQ', '9'.repeat(4000)); await page.waitForTimeout(700);
    assert.equal(asked.filter(a => ['timelineGet', 'poolList', 'findSheets', 'get'].includes(a.op) && a.id.length > 13).length, 0, 'no cloud lookup for a number longer than any Etsy number');
    assert.equal(await page.evaluate(() => /cloud/i.test(document.getElementById('cnsMsg').textContent) && /Looking/.test(document.getElementById('cnsMsg').textContent)), false, 'and no spinner for one');
    console.log('  ✓ 3 "#4176 208 841" pasted finds it; long digit pastes ask the cloud nothing');

    });
    await step('2', async () => {
    /* 2 · the cloud's reads all failing: said as a failure, not as "not there", and asked again next time */
    await page.fill('#cnsQ', '');
    await page.keyboard.type(FAIL);
    await page.waitForFunction(() => !/Looking in the cloud/.test(document.getElementById('cnsMsg').textContent) && /No order has/.test(document.getElementById('cnsMsg').textContent), null, { timeout: 8000 });
    const msg = await page.textContent('#cnsMsg');
    assert.match(msg, /could not be asked/, 'a lookup that failed says so: ' + msg);
    const n1 = asked.filter(a => a.id === FAIL).length;
    await page.fill('#cnsQ', ''); await page.keyboard.type(FAIL);
    await page.waitForFunction(() => !/Looking in the cloud/.test(document.getElementById('cnsMsg').textContent) && /No order has/.test(document.getElementById('cnsMsg').textContent), null, { timeout: 8000 });
    assert(asked.filter(a => a.id === FAIL).length > n1, 'a failed lookup is not kept as "not found": it is asked again');
    console.log('  ✓ 2 a failed cloud lookup reads as a failure and is not cached');
    await page.keyboard.press('Escape'); await page.keyboard.press('Escape');
    await page.waitForFunction(() => document.getElementById('cnSearch').hidden);

    });
    await step('4', async () => {
    /* 4 · Ctrl+K with a window over the order window: nothing moves (no pop-up on a pop-up) */
    await page.evaluate(() => { const r = B.orders.rows.find(x => x.order.receiptId === '4176208841'); OrderWin.open(r.key); });
    await page.waitForFunction(() => OrderWin.isOpen());
    await page.evaluate(() => { const d = document.createElement('dialog'); d.id = '__over'; d.innerHTML = '<p>photo</p><button>ok</button>'; document.body.appendChild(d); d.showModal(); });
    await page.keyboard.press('Control+k'); await page.waitForTimeout(250);
    const st4 = await page.evaluate(() => ({ search: OrderSearch.isOpen(), ow: OrderWin.isOpen(), over: document.getElementById('__over').open }));
    assert.deepEqual(st4, { search: false, ow: true, over: true }, 'Ctrl+K over a window on the order window leaves both as they are: ' + JSON.stringify(st4));
    await page.evaluate(() => { const d = document.getElementById('__over'); d.close(); d.remove(); OrderWin.close(); });
    // the sign-in screen: the search does not open over it
    await page.evaluate(() => document.getElementById('signin').classList.remove('hidden'));
    await page.keyboard.press('Control+k'); await page.waitForTimeout(150);
    const overSignin = await page.evaluate(() => OrderSearch.isOpen());
    await page.evaluate(() => { document.getElementById('signin').classList.add('hidden'); OrderSearch.close(true); });
    assert.equal(overSignin, false, 'the search does not open over the sign-in screen');
    console.log('  ✓ 4 Ctrl+K never stacks the search on another window');

    });
    await step('5', async () => {
    /* 5 · Restore racing reads already in flight */
    records.push(REC('4100000020', 'Cleo', 0.5));
    await page.evaluate(async () => { await Cancelled.load(true); await Cancelled.history(); });
    const flights = () => page.evaluate(() => window.__arr || []);
    await page.evaluate(() => { window.__arr = []; const f = Orders.cancelArrived; Orders.cancelArrived = recs => { window.__arr.push(...recs.map(r => r.orderId)); return f(recs); }; });
    const c0 = await page.evaluate(() => Cancelled.count());
    gate('ids'); gate('list');
    await page.evaluate(() => { window.__l = Cancelled.load(true); window.__h = Cancelled.history(); });
    for (let i = 0; i < 40 && !(holds.ids.seen && holds.list.seen); i++) await page.waitForTimeout(50);
    assert(holds.ids.seen && holds.list.seen, 'both reads in flight');
    await page.evaluate(() => Cancelled.restore('4100000020'));
    holds.ids.open(); holds.list.open(); delete holds.ids; delete holds.list;
    await page.evaluate(async () => { await window.__l; await window.__h; });
    // AutoCancel's newest records, read before the restore too
    await page.evaluate(() => Cancelled.absorb(['4100000020']));
    await page.waitForTimeout(300);
    const r5 = await page.evaluate(() => ({ has: Cancelled.has('4100000020'), n: Cancelled.count(), inList: Cancelled.ids().includes('4100000020') }));
    assert.deepEqual({ has: r5.has, n: r5.n }, { has: false, n: c0 - 1 }, 'a restore stays restored when a read in flight comes back: ' + JSON.stringify(r5) + ' before ' + c0);
    assert.deepEqual(await flights(), [], 'and it does not fly in as a new cancel');
    const hist = await page.evaluate(async () => (await Cancelled.history()).map(c => c.orderId));
    assert(!hist.includes('4100000020'), 'nor come back into the list');
    console.log('  ✓ 5 a restore racing the reads in flight stays restored, counted right, no false arrival');

    });
    await step('6', async () => {
    /* 6 · the tab searched for an older order whose lookup fails: the spinner ends with a message */
    await page.evaluate(() => { OrderSearch.close(true); for (const d of document.querySelectorAll('dialog[open]')) d.close(); });
    await page.evaluate(() => { CN.setMode('orders'); Orders.renderNow(); });
    await page.click('#ordChips [data-pile="cancelled"]');
    await page.waitForFunction(() => document.querySelectorAll('#ordBody .cxRow').length >= 1);
    await page.fill('#ordQ', OLD);
    await page.waitForTimeout(3500);
    const body = await page.evaluate(() => ({ text: document.getElementById('ordBody').textContent.replace(/\s+/g, ' ').trim(), spin: !!document.querySelector('#ordBody .cxWait') }));
    assert(asked.some(a => a.op === 'cancelCheck'), 'the older order was looked up');
    assert.equal(body.spin, false, 'no spinner left once the lookup failed: ' + JSON.stringify(body));
    assert.match(body.text, /could not be read/, 'it says the read failed: ' + JSON.stringify(body));
    console.log('  ✓ 6 a failed lookup in the Cancelled tab ends its spinner with a message');

    });
    assert.equal(etsyCalls(), etsy0, 'no Etsy call');
    assert.deepEqual(errors, [], 'no page errors');
    assert.equal(bad, 0, bad + ' step(s) failed');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('adv-search: ok'), e => { console.error(e); process.exit(1); });
