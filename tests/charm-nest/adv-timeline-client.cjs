// Adversarial checks of the order timeline's client outbox (order-timeline.js, wave 3 area 6). Loopback only.
//   1. Multibyte text: a batch under 60,000 characters but over 64 KB as bytes (Japanese, emoji) was sent as keepalive,
//      which the browser refuses outright; the outbox retried that same batch forever and nothing after it was sent.
//   2. An event whose details cannot be written as JSON (a circular object) stopped the outbox for good: flush threw
//      before its try, `sending` stayed true, and every later event of the page waited forever (and none was saved).
//   3. get() folds in the page's pending events by their id: the server's key is type~cleaned id, so a pending event
//      whose id has a space or "/" was shown twice, and one sharing an id with another type's event was not shown.
//   NODE_PATH=… PW_DIR=… CHROMIUM=… node tests/charm-nest/adv-timeline-client.cjs
const path = require('path'), assert = require('assert/strict'), fs = require('fs'), http = require('http');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const got = [];
let down = false, stored = [];
const srv = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/t.html') { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return res.end('<!doctype html><meta charset="utf-8"><script src="/order-timeline.js"></script>'); }
  if (u.pathname === '/order-timeline.js') { res.writeHead(200, { 'Content-Type': 'text/javascript' }); return res.end(fs.readFileSync(process.env.OT_SRC || path.join(root, 'order-timeline.js'))); }
  if (u.pathname === '/.netlify/functions/charmNestLibrary' && req.method === 'POST') {
    const c = []; req.on('data', d => c.push(d)); req.on('end', () => {
      const b = JSON.parse(Buffer.concat(c).toString('utf8') || '{}');
      res.writeHead(b.op !== 'timelineGet' && down ? 503 : 200, { 'Content-Type': 'application/json' });
      if (b.op === 'timelineGet') return res.end(JSON.stringify({ orderId: b.orderId, events: stored.filter(e => e.orderId === b.orderId), cancelled: null }));
      if (down) return res.end('{"error":"down"}');
      for (const e of b.events || []) got.push({ id: e.id, type: e.type, orderId: e.orderId });
      res.end(JSON.stringify({ ok: true }));
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
  const page = async () => {
    const ctx = await browser.newContext();
    await ctx.route(u => !['127.0.0.1', 'localhost'].includes(u.hostname), r => r.abort());
    const p = await ctx.newPage(); await p.goto(base + '/t.html');
    await p.evaluate(() => OrderTimeline.config({ mode: 'sorter', by: 'Ana' }));
    return { ctx, p };
  };
  try {
    const only = (process.env.ADV_CASES || '123').split(''); let n = 0;
    /* ── 1 · 45 KB of characters, 130 KB of bytes ── */
    if (only.includes(String(++n))) {
      const { ctx, p } = await page();
      await p.evaluate(() => { for (let i = 0; i < 60; i++) OrderTimeline.record({ orderId: '4191000001', type: 'engraveChanged', id: 'j' + i, text: 'エンジェル', data: { engraving: 'あいう'.repeat(300) } }); });
      await until(() => got.filter(g => g.orderId === '4191000001').length === 60, 9000, 'all 60 multibyte events sent');
      await ctx.close();
      console.log('  ✓ a batch under 60,000 characters but over 64 KB of bytes is sent');
    }
    /* ── 2 · a circular detail does not stop the outbox ── */
    if (only.includes(String(++n))) {
      const { ctx, p } = await page();
      await p.evaluate(() => { const circ = { a: 1 }; circ.self = circ; OrderTimeline.record({ orderId: '4191000002', type: 'note', id: 'bad', text: 'odd', data: circ }); });
      await new Promise(r => setTimeout(r, 1500));
      await p.evaluate(() => OrderTimeline.record({ orderId: '4191000002', type: 'note', id: 'next', text: 'the next one' }));
      await until(() => got.some(g => g.id === 'next'), 6000, 'the event after the circular one is sent');
      await ctx.close();
      console.log('  ✓ an event that cannot be written as JSON does not stop every later one');
    }
    /* ── 3 · pending events folded into get() by the server's own key ── */
    if (only.includes(String(++n))) {
      const R = '4191000003', t = Date.now() - 5000;
      stored = [
        { id: `${R}~qrLabel~QR_1_2`, orderId: R, type: 'qrLabel', at: t },          // written, but its answer was lost
        { id: `${R}~released~h1`, orderId: R, type: 'released', at: t - 60000 }      // another type, the same id
      ];
      down = true;
      const { ctx, p } = await page();
      const tl = await p.evaluate(([R, t]) => {
        OrderTimeline.record({ orderId: R, type: 'qrLabel', id: 'QR 1/2', at: t });
        OrderTimeline.record({ orderId: R, type: 'held', id: 'h1', at: t + 1000 });
        return OrderTimeline.get(R);
      }, [R, t]);
      const types = tl.events.map(e => e.type);
      assert.equal(types.filter(x => x === 'qrLabel').length, 1, 'the QR label once, not twice: ' + JSON.stringify(types));
      assert(types.includes('held'), 'the pending hold is shown: ' + JSON.stringify(types));
      down = false; await ctx.close();
      console.log('  ✓ get() folds in pending events by type and cleaned id');
    }
    console.log('adv-timeline-client: all passed');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
