// The stall recorder (charm-nest-hangwatch.js, Paul 14 Nov 2026: "if I leave the sorter running it always crashes ... I get a blank screen"): headless Chromium, a cross-origin isolated page that loads it before
// the background clock, as the sorter does.
//   1 · it changes nothing the page does: timers, intervals, frames, events (and their removal), a worker's onmessage and the clock's own timers run, with their arguments, `this` and return values;
//   2 · a page whose main thread is stuck inside a timer for 26 s is written down by the worker beside it, in IndexedDB: the code it was inside (its source), for how long, the last pieces it ran,
//       and the page's memory samples; when the page goes on, a "resume" record says how long it stalled; a busy page that keeps answering is never written down.
//   PW_DIR=/path/to/node_modules node tests/charm-nest/hangwatch.cjs     (CHROMIUM=/path/to/chrome when playwright's own is not installed)
'use strict';
const assert = require('assert'), http = require('http'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '..', '..');
let pw; try { pw = require(process.env.PW_DIR ? path.join(process.env.PW_DIR, 'playwright-core') : 'playwright-core'); } catch (e) { console.log('hangwatch: playwright-core is not installed here: not run'); process.exit(0); }
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const PAGE = `<!doctype html><html><head><meta charset="utf-8"><title>t</title>
<script src="/charm-nest-hangwatch.js"></script><script src="/charm-nest-clock.js"></script></head><body><div id="x"></div></body></html>`;
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0], h = { 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp', 'Cache-Control': 'no-store' };
  if (u === '/' || u === '/index.html') { res.writeHead(200, Object.assign({ 'Content-Type': 'text/html' }, h)); return res.end(PAGE); }
  const f = path.join(root, path.basename(u));
  if (/^\/charm-nest-(hangwatch|clock)\.js$/.test(u) && fs.existsSync(f)) { res.writeHead(200, Object.assign({ 'Content-Type': 'text/javascript' }, h)); return res.end(fs.readFileSync(f)); }
  res.writeHead(404, h); res.end('no');
});
(async () => {
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const port = server.address().port;
  const opts = { headless: true, args: ['--no-sandbox'] }; if (process.env.CHROMIUM) opts.executablePath = process.env.CHROMIUM;
  const browser = await pw.chromium.launch(opts), ctx = await browser.newContext(), page = await ctx.newPage();
  const errs = []; page.on('pageerror', e => errs.push(String(e && e.message || e)));
  await page.goto('http://127.0.0.1:' + port + '/');
  await page.waitForFunction(() => window.CharmNestHang && window.CharmNestHang.active === true, null, { timeout: 10000 });
  ok(await page.evaluate(() => window.crossOriginIsolated && window.CharmNestHang.active), 'the recorder is on in a page that may share memory with its worker');
  // 1 · nothing the page does is changed
  const r = await page.evaluate(() => new Promise(done => {
    const seen = {}; const self0 = {};
    const id1 = setTimeout(function (a, b) { seen.timeout = [a, b, this === window]; }, 5, 'a', 'b');
    let ticks = 0; const id2 = setInterval(() => { ticks++; if (ticks === 3) { clearInterval(id2); seen.interval = ticks; } }, 10);
    const id3 = setTimeout(() => { seen.cleared = 'ran'; }, 20); clearTimeout(id3);
    requestAnimationFrame(t => { seen.frame = typeof t; });
    const el = document.getElementById('x'); let clicks = 0; const onClick = function (e) { clicks++; seen.eventThis = this === el; seen.eventType = e.type; };
    el.addEventListener('click', onClick); el.click(); el.removeEventListener('click', onClick); el.click();
    const w = new Worker(URL.createObjectURL(new Blob(['onmessage=e=>postMessage(e.data*2)'], { type: 'text/javascript' })));
    w.onmessage = function (e) { seen.worker = e.data; seen.workerHandler = typeof w.onmessage === 'function' && this === w; w.terminate(); };
    w.postMessage(21);
    setTimeout(() => done({ seen, clicks, idType: typeof id1, ticks }), 400);
  }));
  ok(r.seen.timeout && r.seen.timeout[0] === 'a' && r.seen.timeout[1] === 'b' && r.seen.timeout[2] === true && r.idType === 'number', 'setTimeout runs its callback with its arguments and `this`, and still returns an id');
  ok(r.seen.interval === 3 && !r.seen.cleared, 'setInterval repeats and stops when cleared; a cleared timeout never runs');
  ok(r.seen.frame === 'number', 'requestAnimationFrame calls back with its time');
  ok(r.clicks === 1 && r.seen.eventThis === true && r.seen.eventType === 'click', 'an event listener runs with its `this` and event, and removeEventListener removes it');
  ok(r.seen.worker === 42 && r.seen.workerHandler === true, 'a worker\'s onmessage handler runs, and reads back as the function that was set');
  // 2 · a busy page that keeps answering is not a stall
  await page.evaluate(() => { const end = Date.now() + 3000; const spin = () => { const t = Date.now() + 200; while (Date.now() < t) {} if (Date.now() < end) setTimeout(spin, 10); }; setTimeout(spin, 0); });
  await page.waitForTimeout(3600);
  let rep = await page.evaluate(() => window.CharmNestHang.report());
  ok(!rep.records.some(x => x.kind === 'stall'), 'a page that is busy but keeps answering is never written down');
  // 3 · a page stuck inside one timer for 26 s
  await page.evaluate(() => { setTimeout(function stuckInHere() { const t = Date.now(); while (Date.now() - t < 26000) { /* stuck */ } }, 0); });
  await page.waitForTimeout(24000);   // (the page is stuck: the browser cannot answer evaluate until it goes on; this just waits)
  await page.waitForFunction(() => window.CharmNestHang && window.CharmNestHang.report, null, { timeout: 30000 });
  await page.waitForTimeout(2500);
  rep = await page.evaluate(() => window.CharmNestHang.report());
  const st = rep.records.filter(x => x.kind === 'stall'), rs = rep.records.filter(x => x.kind === 'resume');
  ok(st.length === 1 && st[0].stalledSec >= 20, 'the stall is written down once, after 20 s: ' + (st[0] ? st[0].stalledSec + ' s' : 'none'));
  ok(st[0].inside && /stuckInHere/.test(st[0].inside[1]) && /^(timeout|interval): /.test(st[0].inside[1]), 'it names the code the page was stuck inside: ' + (st[0].inside ? st[0].inside[1].slice(0, 70) : 'none'));
  ok(st[0].insideForMs >= 18000, 'and for how long it had been inside it: ' + st[0].insideForMs + ' ms');
  ok(Array.isArray(st[0].recent) && st[0].recent.length >= 3 && st[0].recent.every(x => Array.isArray(x) && typeof x[1] === 'string'), 'with the last pieces of code it ran');
  ok(Array.isArray(st[0].samples), 'and the page\'s memory samples');
  ok(rs.length === 1 && rs[0].stalledSec >= 20, 'when the page goes on, a "resume" record says how long it was stuck: ' + (rs[0] ? rs[0].stalledSec + ' s' : 'none'));
  ok(errs.length === 0, 'no page error in the whole run: ' + errs.join(' | '));
  await browser.close(); server.close();
  console.log('hangwatch: ' + n + ' checks passed');
})().catch(e => { console.error(e); try { server.close(); } catch (_) {} process.exit(1); });
