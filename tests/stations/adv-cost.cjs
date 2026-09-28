// Adversarial test (cost and limits) of the station client in headless Chromium, every request stubbed here:
//   · a Sorting batch of 25 orders scanned at once asks whether they are cancelled in ONE request (was 25 function calls,
//     each able to miss the 2.5 s answer), and the cancelled one still raises its answer; later single scans ask alone
//   · 70 at once: two requests (the server answers 60 at a time)
//   · a server refusing every event for a long time: the outbox in memory stays bounded (was unbounded; 500 kept in storage)
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/adv-cost.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const ORIGIN = 'http://cost.test';
const ids = Array.from({ length: 25 }, (_, i) => String(3900000000 + i)), CANCELLED = ids[7];
const st = { checks: [], posts: 0, refuse: false };

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext();
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url());
    if (u.origin !== ORIGIN) return r.abort();
    if (u.pathname === '/') return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><body><script src="/order-timeline.js"></script><script src="/station-timeline.js"></script></body>' });
    if (u.pathname === '/.netlify/functions/firebaseOrders') {
      const q = u.searchParams.get('cancelCheck');
      if (q) { st.checks.push(q.split(',')); const c = q.split(',').includes(CANCELLED) ? { [CANCELLED]: { at: Date.now() - 6e5, by: 'Etsy', why: 'Cancelled on Etsy', source: 'etsy' } } : {}; return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, cancelled: c }) }); }
      st.posts++;
      if (st.refuse) return r.fulfill({ status: 500, contentType: 'application/json', body: '{"error":"pretend outage"}' });
      return r.fulfill({ status: 200, contentType: 'application/json', body: '{"success":true}' });
    }
    const f = path.join(root, u.pathname);
    if (f.startsWith(root) && fs.existsSync(f)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(f) });
    return r.fulfill({ status: 404, body: '' });
  });
  const page = await ctx.newPage();
  await page.goto(ORIGIN + '/');
  await page.evaluate(() => StationTimeline.init({ station: 'sorting', device: 'sorting-1', getEmployee: () => 'Maya' }));

  // ── a batch of 25 scanned at once ──
  const res = await page.evaluate(ids => Promise.all(ids.map(id => StationTimeline.scanned(id, { how: 'sheet QR', quiet: true }))), ids);
  assert.strictEqual(st.checks.length, 1, `one cancel check for the batch, not ${st.checks.length}`);
  assert.deepStrictEqual(st.checks[0].slice().sort(), ids.slice().sort(), 'asking about every order of the batch');
  assert.deepStrictEqual(res.map((x, i) => x.cancelled ? ids[i] : null).filter(Boolean), [CANCELLED], 'the cancelled order is still found');
  // ── single scans later each ask alone ──
  await page.evaluate(id => StationTimeline.scanned(id, { quiet: true }), ids[0]);
  await page.evaluate(id => StationTimeline.scanned(id, { quiet: true }), ids[1]);
  assert.strictEqual(st.checks.length, 3, 'a scan on its own is its own check');
  // ── 70 at once: two requests of at most 60 ──
  const more = Array.from({ length: 70 }, (_, i) => String(3910000000 + i));
  await page.evaluate(ids => Promise.all(ids.map(id => StationTimeline.scanned(id, { quiet: true }))), more);
  assert.deepStrictEqual(st.checks.slice(3).map(c => c.length), [60, 10], 'two checks, the server answers 60 at a time');
  console.log(`batch: 25 orders → 1 cancel check (was 25); 70 → 2; the cancelled one found`);

  // ── the outbox while the server refuses every event ──
  await page.waitForFunction(() => OrderTimeline.pending() === 0, null, { timeout: 8000 });
  st.refuse = true;
  const pend = await page.evaluate(() => { for (let i = 0; i < 2600; i++) OrderTimeline.record({ orderId: '3920000000', type: 'note', text: 'n' + i, id: 'n' + i }); return { mem: OrderTimeline.pending(), kept: JSON.parse(localStorage.getItem('orderTimeline.outbox.v1')).length, last: JSON.parse(localStorage.getItem('orderTimeline.outbox.v1')).pop().id }; });
  assert(pend.mem <= 2000, `the outbox in memory stays bounded: ${pend.mem}`);
  assert.strictEqual(pend.kept, 500, 'and 500 are kept across a reload, as before');
  assert.strictEqual(pend.last, 'n2599', 'the newest are the ones kept');
  st.refuse = false;
  await page.evaluate(() => OrderTimeline.flush());
  console.log(`outbox: 2600 events while refused → ${pend.mem} in memory (was 2600, growing), ${pend.kept} in storage`);

  await browser.close();
  console.log('adv-cost (stations): all passed');
})().catch(e => { console.error(e); process.exit(1); });
