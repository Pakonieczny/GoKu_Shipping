// Adversarial checks of the order timeline's outbox (order-timeline.js, task G, 28 Sep). The sorter and every station
// page are one site, so they share one localStorage and one outbox key. A tiny loopback server answers the two doors
// (charmNestLibrary timelineAdd, which takes every type; firebaseOrders, which keeps station types only, as the real one).
//   1. A station page that opens with sorter events in the outbox (queued by the sorter while it could not send) must
//      leave them for the sorter: sent through the station's door they were dropped, and the outbox forgot them.
//   2. A batch bigger than 64 KB goes (a keepalive request over that is refused by the browser, so the outbox retried the
//      same batch forever and nothing after it was ever sent).
//   3. Two sorter tabs: what one tab queued is not wiped from the outbox by the other tab's save.
// No network beyond loopback.
//   NODE_PATH=… PW_DIR=… CHROMIUM=… node tests/charm-nest/adv-timeline-outbox.cjs
const path = require('path'), assert = require('assert/strict'), fs = require('fs'), http = require('http');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const STATION = new Set(['scan', 'sorted', 'welded', 'assembled', 'packed', 'labelPrinted', 'shipped', 'etsyCompleted', 'cancelAlert', 'note']);

const got = [];   // { door, id, type } of every event a door kept
let down = false;
const srv = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/t.html') { res.writeHead(200, { 'Content-Type': 'text/html' }); return res.end('<!doctype html><meta charset="utf-8"><script src="/order-timeline.js"></script>'); }
  if (u.pathname === '/order-timeline.js') { res.writeHead(200, { 'Content-Type': 'text/javascript' }); return res.end(fs.readFileSync(process.env.OT_SRC || path.join(root, 'order-timeline.js'))); }
  const m = /^\/\.netlify\/functions\/(\w+)$/.exec(u.pathname);
  if (m && req.method === 'POST') {
    const c = []; req.on('data', d => c.push(d)); req.on('end', () => {
      if (down) { res.writeHead(503, { 'Content-Type': 'application/json' }); return res.end('{"error":"down"}'); }
      const b = JSON.parse(Buffer.concat(c).toString('utf8') || '{}'), list = m[1] === 'firebaseOrders' ? b.timeline : b.events;
      for (const e of list || []) if (m[1] !== 'firebaseOrders' || STATION.has(e.type)) got.push({ door: m[1], id: e.id, type: e.type, orderId: e.orderId });
      res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ ok: true, success: true }));
    });
    return;
  }
  res.writeHead(404); res.end();
});
const until = async (fn, ms = 6000, what = '') => { const t0 = Date.now(); while (Date.now() - t0 < ms) { if (await fn()) return; await new Promise(r => setTimeout(r, 100)); } throw new Error('timed out: ' + what); };
const OUTBOX = 'orderTimeline.outbox.v1';

(async () => {
  await new Promise(r => srv.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    /* ── 1 · a station page opening on the sorter's queued events ── */
    const only = (process.env.ADV_CASES || '123').split('');
    if (only.includes('1')) {
      const ctx = await browser.newContext();
      await ctx.route(u => !['127.0.0.1', 'localhost'].includes(u.hostname), r => r.abort());
      const queued = ['held', 'renested', 'qrLabel'].map((type, i) => ({ orderId: '4189000001', type, id: 'q' + i, at: Date.now() - 60000 + i, by: 'Ana', station: '', device: '', sandbox: false }));
      await ctx.addInitScript(([k, v]) => { if (!sessionStorage.getItem('seeded')) { sessionStorage.setItem('seeded', '1'); localStorage.setItem(k, v); } }, [OUTBOX, JSON.stringify(queued)]);
      const st = await ctx.newPage();
      await st.goto(base + '/t.html');
      await st.evaluate(() => { OrderTimeline.config({ mode: 'station', station: 'sorting', device: 'sorting', by: 'Ben' }); OrderTimeline.record({ orderId: '4189000001', type: 'scan', id: 's1' }); });
      await until(() => got.some(g => g.id === 's1'), 6000, 'the scan is sent');
      await new Promise(r => setTimeout(r, 1800));   // (a second pass of the station's outbox, if it had one)
      const left = await st.evaluate(k => JSON.parse(localStorage.getItem(k) || '[]').map(e => e.id), OUTBOX);
      assert.deepEqual(got.filter(g => g.id !== 's1'), [], 'the station sent none of the sorter\'s events');
      assert.deepEqual(left.sort(), ['q0', 'q1', 'q2'], 'the sorter\'s events wait in the outbox for the sorter');
      // the sorter opens: it sends them through its own door, and they leave the outbox
      const so = await ctx.newPage();
      await so.goto(base + '/t.html');
      await so.evaluate(() => OrderTimeline.config({ mode: 'sorter', by: 'Ana' }));
      await until(() => ['q0', 'q1', 'q2'].every(id => got.some(g => g.id === id && g.door === 'charmNestLibrary')), 6000, 'the sorter sends its events');
      await st.evaluate(() => OrderTimeline.record({ orderId: '4189000001', type: 'sorted', id: 's2' }));
      await until(() => got.some(g => g.id === 's2'), 6000, 'the next scan');
      await until(async () => (await so.evaluate(k => JSON.parse(localStorage.getItem(k) || '[]').length, OUTBOX)) === 0, 6000, 'the outbox empties');
      await ctx.close();
      console.log('  ✓ a station page leaves the sorter\'s queued events for the sorter; the sorter sends them');
    }
    got.length = 0;

    /* ── 2 · fifty events of ~1.8 KB each: over the 64 KB a keepalive request may carry ── */
    if (only.includes('2')) {
      const ctx = await browser.newContext();
      await ctx.route(u => !['127.0.0.1', 'localhost'].includes(u.hostname), r => r.abort());
      const p = await ctx.newPage();
      await p.goto(base + '/t.html');
      await p.evaluate(() => { OrderTimeline.config({ mode: 'sorter', by: 'Ana' }); for (let i = 0; i < 60; i++) OrderTimeline.record({ orderId: '4189000002', type: 'merged', id: 'm' + i, text: 'x'.repeat(190), data: { poolIds: Array.from({ length: 40 }, (_, j) => `4189000002_7000${j}_${i}`), note: 'y'.repeat(300) } }); });
      await until(() => got.filter(g => g.orderId === '4189000002').length === 60, 8000, 'all 60 sent');
      assert.equal(await p.evaluate(() => OrderTimeline.pending()), 0);
      await ctx.close();
      console.log('  ✓ a batch over 64 KB is sent (in parts), nothing stuck behind it');
    }
    got.length = 0;

    /* ── 3 · two sorter tabs, the server down: neither tab's save wipes what the other queued ── */
    if (only.includes('3')) {
      const ctx = await browser.newContext();
      await ctx.route(u => !['127.0.0.1', 'localhost'].includes(u.hostname), r => r.abort());
      down = true;
      const a = await ctx.newPage(), b = await ctx.newPage();
      await a.goto(base + '/t.html'); await b.goto(base + '/t.html');
      for (const p of [a, b]) await p.evaluate(() => OrderTimeline.config({ mode: 'sorter', by: 'Ana' }));
      await a.evaluate(() => OrderTimeline.record({ orderId: '4189000003', type: 'held', id: 'a1' }));
      await b.evaluate(() => OrderTimeline.record({ orderId: '4189000003', type: 'released', id: 'b1' }));
      await a.evaluate(() => OrderTimeline.record({ orderId: '4189000003', type: 'note', id: 'a2', text: 'n' }));
      const stored = await a.evaluate(k => JSON.parse(localStorage.getItem(k) || '[]').map(e => e.id).sort(), OUTBOX);
      assert.deepEqual(stored, ['a1', 'a2', 'b1'], 'tab B\'s event is still in the outbox after tab A saved');
      await b.close();                                   // tab B goes before it could send
      down = false;
      await a.reload(); await a.evaluate(() => OrderTimeline.config({ mode: 'sorter', by: 'Ana' }));
      await until(() => ['a1', 'a2', 'b1'].every(id => got.some(g => g.id === id)), 8000, 'all three sent after the reload');
      await ctx.close();
      console.log('  ✓ two tabs: each keeps the other\'s queued events');
    }
    console.log('adv-timeline-outbox: all passed');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
