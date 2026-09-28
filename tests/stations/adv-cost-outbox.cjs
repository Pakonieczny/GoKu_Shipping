// Adversarial test (cost, limits, long sessions) of the timeline outbox and the station cancel check, in headless
// Chromium with every request stubbed here (no network):
//   · events recorded less than a second apart (a scanner kept going) are sent while it keeps on: each new event used to
//     put the send off again, so nothing left the page until it stopped, and past 2000 the oldest were dropped
//   · the outbox's memory of what it delivered stays bounded over a long session (it kept every key ever sent)
//   · a station's "not cancelled" answers older than the 30 s they are used for are let go (it kept every order scanned)
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/adv-cost-outbox.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const ORIGIN = 'http://cost-outbox.test';
const st = { posts: [], checks: 0 };
// test-only peeks at two private collections (the test fails loudly if the source no longer has them)
const expose = (name, src) => {
  const out = name === 'order-timeline.js' ? src.replace('sent = new Set();', 'sent = window.__sent = new Set();')
    : name === 'station-timeline.js' ? src.replace('const cleared = new Map();', 'const cleared = window.__cleared = new Map();') : src;
  assert(out !== src, 'test hook not found in ' + name);
  return out;
};

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext();
  await ctx.addInitScript(() => { const real = Date.now; let off = 0; Date.now = () => real() + off; window.__advance = ms => { off += ms; }; });
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url());
    if (u.origin !== ORIGIN) return r.abort();
    if (u.pathname === '/') return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><body><script src="/order-timeline.js"></script><script src="/station-timeline.js"></script></body>' });
    if (u.pathname === '/.netlify/functions/firebaseOrders') {
      if (u.searchParams.get('cancelCheck')) { st.checks++; return r.fulfill({ status: 200, contentType: 'application/json', body: '{"success":true,"cancelled":{}}' }); }
      st.posts.push({ at: Date.now(), n: (JSON.parse(r.request().postData() || '{}').timeline || []).length });
      return r.fulfill({ status: 200, contentType: 'application/json', body: '{"success":true}' });
    }
    const name = u.pathname.slice(1);
    if (['order-timeline.js', 'station-timeline.js'].includes(name)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: expose(name, fs.readFileSync(path.join(root, name), 'utf8')) });
    return r.fulfill({ status: 404, body: '' });
  });
  const page = await ctx.newPage();
  await page.goto(ORIGIN + '/');
  await page.evaluate(() => StationTimeline.init({ station: 'welding', device: 'weld-1', getEmployee: () => 'Maya' }));

  // ── a scanner that keeps going: an event every 300 ms for 4.5 s ──
  const t0 = Date.now();
  await page.evaluate(() => new Promise(done => { let i = 0; const t = setInterval(() => {
    OrderTimeline.record({ orderId: String(3930000000 + i), type: 'scan', text: 's' + i, id: 's' + i });
    if (++i >= 15) { clearInterval(t); done(); } }, 300); }));
  const during = st.posts.filter(p => p.at <= t0 + 4500).length;
  assert(during >= 2, `events are sent while the scanner keeps on (${during} sends during 4.5 s; was 0 until it stopped)`);
  await page.waitForFunction(() => OrderTimeline.pending() === 0, null, { timeout: 8000 });
  assert.strictEqual(st.posts.reduce((a, p) => a + p.n, 0), 15, 'every event sent once');
  console.log(`steady scanning: ${during} sends while it kept on (was 0), all 15 events delivered`);

  // ── a long session: what was delivered is not remembered forever ──
  for (let k = 0; k < 4; k++) {
    await page.evaluate(k => { for (let i = 0; i < 1000; i++) OrderTimeline.record({ orderId: '3940000000', type: 'note', text: 'n', id: `n${k}-${i}` }); }, k);
    await page.waitForFunction(() => OrderTimeline.pending() === 0, null, { timeout: 30000 });
  }
  const sent = await page.evaluate(() => window.__sent.size);
  assert(sent <= 3000, `delivered keys kept in memory stay bounded: ${sent} (was 4015, growing)`);
  console.log(`4015 events delivered → ${sent} keys kept (was every one)`);

  // ── a station open for weeks: old "not cancelled" answers go ──
  const many = Array.from({ length: 700 }, (_, i) => String(3950000000 + i));
  await page.evaluate(ids => Promise.all(ids.map(id => StationTimeline.scanned(id, { quiet: true }))), many);
  const before = await page.evaluate(() => window.__cleared.size);
  await page.evaluate(() => window.__advance(31000));
  await page.evaluate(() => StationTimeline.scanned('3959999999', { quiet: true }));
  const after = await page.evaluate(() => window.__cleared.size);
  assert(after <= 500, `answers older than 30 s are let go: ${after} kept (was ${before + 1}, growing)`);
  // the guard still skips a fresh answer: an order scanned just now is not asked again
  const checks = st.checks;
  await page.evaluate(() => StationTimeline.guard('3959999999', { quiet: true }));
  assert.strictEqual(st.checks, checks, 'a fresh "not cancelled" answer is still used by the guard');
  console.log(`cancel answers: ${before} → ${after} after 31 s; a fresh one still spares the guard's check`);

  await browser.close();
  console.log('adv-cost-outbox (stations): all passed');
})().catch(e => { console.error(e); process.exit(1); });
