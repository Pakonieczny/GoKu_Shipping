// Adversarial (stations, 15 Oct 2026): the efficiency events a station page queues while the network is down (station-activity.js).
// The Charm Sorter and every station page keep their unsent events in ONE localStorage list per page (station_activity_q.<device>), so two tabs
// of the same page (two sorter windows, a station page opened twice) share it.
//   1 · two tabs, offline: tab A queues an event, tab B queues one after it, tab A is reloaded before the network is back. Every event reaches
//       the server once: B's save used to write only its own list over the disk's, and A's unsent event was gone after A's reload
//   2 · the answer to a batch is lost (the server wrote it, the page never heard): the retry sends the same ids, the server keeps each once
//   3 · a tab closed with events unsent: the tab still open delivers them (it used to wait for a reload), and the disk list empties
// Loopback only. NODE_PATH=… PW_DIR=… CHROMIUM=… node tests/stations/adv-activity-tabs.cjs   (run through /tmp/cn-run.sh on the shared box)
'use strict';
const path = require('path'), assert = require('assert/strict'), fs = require('fs'), http = require('http');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const stored = new Map();       // event id → how many times the server was sent it
let loseAnswers = false;        // the server writes the batch, then the answer never arrives
let down = false;               // the server answers 503 (the shop's line is down; a page cannot be reloaded under a browser-level offline switch)
const PAGE = `<!doctype html><meta charset="utf-8"><script>
window.StationSession = { who: () => ({ person: 'Tess Sorter', station: 'sorting', device: 'sorting-1', computer: 'pc-ABCDEFGHJKMN', session: 'sorting-1-ABCD-k1-XYZW', startAt: Date.now() - 60000 }),
  page: () => ({ station: 'sorting', device: 'sorting-1', computer: 'pc-ABCDEFGHJKMN' }) };
</script><script src="/station-activity.js"></script>`;
const srv = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/t.html') { res.writeHead(200, { 'Content-Type': 'text/html' }); return res.end(PAGE); }
  if (u.pathname === '/station-activity.js') { res.writeHead(200, { 'Content-Type': 'text/javascript' }); return res.end(fs.readFileSync(path.join(root, 'station-activity.js'))); }
  if (u.pathname === '/.netlify/functions/firebaseOrders' && req.method === 'POST') {
    const c = []; req.on('data', d => c.push(d)); req.on('end', () => {
      if (down) { res.writeHead(503, { 'Content-Type': 'application/json' }); return res.end('{"error":"down"}'); }
      const b = JSON.parse(Buffer.concat(c).toString('utf8') || '{}');
      for (const e of b.activity || []) stored.set(e.id, (stored.get(e.id) || 0) + 1);
      if (loseAnswers && b.activity) { req.socket.destroy(); return; }
      res.writeHead(200, { 'Content-Type': 'application/json' }); res.end('{"success":true}');
    });
    return;
  }
  res.writeHead(404); res.end();
});
const until = async (fn, ms = 8000, what = '') => { const t0 = Date.now(); while (Date.now() - t0 < ms) { if (await fn()) return; await new Promise(r => setTimeout(r, 100)); } throw new Error('timed out: ' + what); };
const QKEY = 'station_activity_q.sorting-1';
const log = (p, orderId) => p.evaluate(o => { StationActivity.log('scan', { orderId: o, parts: 1 }); return StationActivity.pending(); }, orderId);

(async () => {
  await new Promise(r => srv.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fresh = async () => { const ctx = await browser.newContext(); await ctx.route(u => !['127.0.0.1', 'localhost'].includes(u.hostname), r => r.abort()); return ctx; };
  const queuedOn = async (p) => p.evaluate(k => JSON.parse(localStorage.getItem(k) || '[]').map(e => e.orderId).sort(), QKEY);
  try {
    /* ── 1 · two tabs offline, one reloaded ── */
    {
      stored.clear();
      const ctx = await fresh(), a = await ctx.newPage(), b = await ctx.newPage();
      await a.goto(base + '/t.html'); await b.goto(base + '/t.html');
      down = true;
      await log(a, '4180000001');                    // both tabs have read the disk now
      await log(b, '4180000002');
      await log(a, '4180000003');                    // A's second event
      await log(b, '4180000004');                    // B saves after A: its list is the whole disk list
      assert.deepEqual(await queuedOn(a), ['4180000001', '4180000002', '4180000003', '4180000004'], 'the disk list holds every tab\'s unsent events');
      await b.close();                               // B's window goes before the network is back
      await a.reload();                              // A is reloaded (F5) while still offline: it can only read the disk
      down = false;
      await a.evaluate(() => StationActivity.flush(true));
      await until(() => stored.size >= 4, 14000, 'every event is sent');
      assert.equal(stored.size, 4, 'four events, none lost');
      for (const [id, n] of stored) assert.ok(n >= 1, id);
      await ctx.close();
      console.log('  ok 1 two tabs, offline, one reloaded: all four efficiency events reach the server');
    }

    /* ── 2 · the answer is lost, the retry sends the same ids ── */
    {
      stored.clear(); loseAnswers = true;
      const ctx = await fresh(), a = await ctx.newPage();
      await a.goto(base + '/t.html');
      await log(a, '4180000011'); await log(a, '4180000012');
      await a.evaluate(() => StationActivity.flush(true));
      await until(() => stored.size === 2, 6000, 'the server got both');
      loseAnswers = false;
      await a.evaluate(() => { return new Promise(r => setTimeout(r, 50)); });
      await a.evaluate(() => StationActivity.flush(true));
      await until(async () => (await a.evaluate(() => StationActivity.pending())) === 0, 14000, 'the retry is acknowledged');
      assert.equal(stored.size, 2, 'the retry carries the same two ids, not new ones');
      await ctx.close();
      console.log('  ok 2 a lost answer: the retry repeats the same ids (the server keeps each once)');
    }

    /* ── 3 · a tab that is gone leaves events behind: a tab still open delivers them, and nothing is left on the disk ── */
    {
      stored.clear();
      const ctx = await fresh(), a = await ctx.newPage(), b = await ctx.newPage();
      await a.goto(base + '/t.html'); await b.goto(base + '/t.html');
      await log(a, '4180000021'); await log(b, '4180000022');       // both read the disk
      down = true;
      await log(b, '4180000023');                                    // B holds events A does not have in memory
      await b.close();                                               // B's window is gone before the line is back
      down = false;
      await a.evaluate(() => StationActivity.flush(true));
      await until(() => stored.size === 3, 14000, 'the open tab delivers its own and the dead tab\'s events');
      await until(async () => (await queuedOn(a)).length === 0, 14000, 'the disk list empties');
      assert.equal(await a.evaluate(() => StationActivity.pending()), 0);
      await ctx.close();
      console.log('  ok 3 a closed tab\'s unsent events are delivered by the tab still open, once, and the disk list empties');
    }
    console.log('adv-activity-tabs: all passed');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
