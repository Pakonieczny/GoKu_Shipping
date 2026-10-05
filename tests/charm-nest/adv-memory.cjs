// Adversarial (wave 4, item 9): memory growth in the sorter. Three records in charm-nest-bridge.js kept one entry for
// every order line the session ever saw: what each line was last read as (the "interpreted" event), the questions a
// line raised (needsDecision) and whether a line was skipped. Twelve pulls of 40 new lines each must leave them holding
// only the lines on the list now, and the timeline must still behave: a line on the list keeps its entries, a skip made
// now is recorded once, and a line that comes back after being dropped sends nothing twice.
// No network but the loopback; no Etsy, no AI.
//   node tests/charm-nest/adv-memory.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000), PER = 40, PULLS = 12;
const order = (i) => { const rid = String(4200000000 + i), tid = rid + '1';
  return { receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + i }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
    lines: [{ transactionId: tid, listingId: '18' + rid.slice(-6), sku: 'NO_SUCH_' + i, title: 'Charm necklace ' + i, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] }; };

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [], results = [];
  const check = async (name, fn) => { try { await fn(); results.push(['ok', name]); console.log('  ✓ ' + name); } catch (e) { results.push(['FAIL', name]); console.log('  ✗ ' + name + '\n      ' + String(e.message).split('\n')[0]); } };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(s => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; },
      { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' });
    const page = await context.newPage();
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // every event the sorter hands the timeline, seen as it goes (and still sent on to the fake server)
    await page.evaluate(() => { window.__ev = []; const rec = OrderTimeline.record.bind(OrderTimeline); OrderTimeline.record = e => { window.__ev.push({ type: e.type, key: e.lineKey || '' }); return rec(e); }; });
    const drained = () => page.waitForFunction(() => CNTimeline.pending() === 0);
    const pull = (from) => page.evaluate(({ orders }) => {
      B.orders.rows = orders.flatMap(o => o.lines.map(l => ({ key: CharmNestOrders.lineKey(o, l), order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null })));
      B.orders.byKey = new Map(B.orders.rows.map(r => [r.key, r]));
      Orders.interpretAll();
    }, { orders: Array.from({ length: PER }, (_, i) => order(from + i)) });
    const sizes = () => page.evaluate(() => Object.assign({ read: Orders._kept(), lines: Orders.rows().length, asks: Orders.rows().reduce((n, r) => n + (r.problems || []).length, 0) }, Review._kept()));

    await page.evaluate(() => Orders.loadMaps(true));
    for (let p = 0; p < PULLS; p++) { await pull(p * PER); await drained(); }
    const s = await sizes();
    console.log('  after', PULLS, 'pulls of', PER, 'lines:', JSON.stringify(s));

    await check('what a line was read as is kept for the lines on the list only', async () => {
      assert.ok(s.read <= s.lines, `${s.read} kept for ${s.lines} lines on the list`);
      assert.equal(s.read, s.lines, 'each line on the list keeps its own');
    });
    await check('the questions a line raised are kept for the lines on the list only', async () => {
      assert.ok(s.asks > 0, 'the lines raise questions (unknown SKUs)');
      assert.ok(s.asked <= s.asks, `${s.asked} kept for ${s.asks} questions on the list`);
    });
    await check('whether a line was skipped is kept for the lines on the list only', async () => assert.ok(s.skip <= s.lines, `${s.skip} kept for ${s.lines} lines on the list`));

    await check('a line on the list skipped now is recorded once', async () => {
      const key = await page.evaluate(() => { const r = Orders.rows()[3]; r.state = 'skipped'; r.reason = 'piece skipped by Test Operator'; Review.syncOrderItems(); Review.syncOrderItems(); return r.key; });
      await drained();
      const n = await page.evaluate(k => window.__ev.filter(e => e.type === 'skipped' && e.key === k).length, key);
      assert.equal(n, 1);
    });
    await check('a line dropped and pulled again sends nothing twice', async () => {
      const before = await page.evaluate(() => window.__ev.length);
      await pull(0); await drained();
      const again = await page.evaluate(b => window.__ev.slice(b).filter(e => ['interpreted', 'needsDecision'].includes(e.type)).length, before);
      assert.equal(again, 0);
      const s2 = await sizes(); assert.equal(s2.read, s2.lines);
    });
    await check('no page errors', async () => assert.deepEqual(errors, []));
    await context.close();
  } finally { await browser.close(); srv.close(); }
  const bad = results.filter(r => r[0] !== 'ok');
  console.log(bad.length ? `\n${bad.length} of ${results.length} failed` : `\nall ${results.length} passed`);
  if (bad.length) process.exitCode = 1;
}
main().catch(e => { console.error(e); process.exit(1); });
