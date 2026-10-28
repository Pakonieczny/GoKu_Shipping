// SERVICE WORKER (local-first plan, Phase 4; Paul, 28 Oct 2026: "Every sheet that opens must be instantaneous ... This must work like a native application"). charm-nest-sw.js
// keeps the page's versioned files (?v=) and its versioned sheet pictures on this computer. The real sorter page (charm-nest-1.html) in headless Chromium over bridge-server.cjs,
// with the static files answered as Netlify answers them in production (ETag; the page no-store, charm-nest-*.js no-cache, the rest max-age=0 + revalidate: a reload without the
// worker revalidates every file), every request that reaches the server counted. Gates:
//   W1 a warm reload makes ≤ 3 static requests (today ≈ 150)            W2 a new ?v= token for one file fetches that file only, and the older copy is dropped
//   W3 the page (HTML) is never answered by the worker                  W4 no function call is answered by the worker; a versioned production picture is read once, a sandbox or unversioned one never kept
//   W5 the off switches: the page's CN_SW = "off", ?sw=off / ?sw=on, and RETIRED in the worker each unregister it and delete its caches
//   W6 the page boots and works with no worker file (404) and with a worker that fails to install; automation runs without a worker unless asked (cn.sw = "on")
// The numbers printed: static requests and time to the first usable screen, cold and warm, without and with the worker (the load average beside them).
//   node tests/charm-nest/service-worker.cjs           PW_DIR=<playwright node_modules> CHROMIUM=<chrome> (run it through slot.sh)
'use strict';
const path = require('path'), fs = require('fs'), os = require('os'), http = require('http'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const SW = require(path.join(root, 'charm-nest-sw.js'));
let failed = 0; const ok = (c, m) => { console.log(`  ${c ? '✓' : '✗'} ${m}`); if (!c) failed++; };

// ── the pure parts, in node: what is kept and what is dropped ──
{
  const o = { origin: 'https://brites-charm-sorter.goldenspike.app' }, at = p => o.origin + p;
  const pic = (p, v) => at('/.netlify/functions/charmNestAsset?url=' + encodeURIComponent(`https://firebasestorage.googleapis.com/v0/b/b/o/${encodeURIComponent(p)}?alt=media&token=t${v ? '&v=' + v : ''}`));
  const table = [
    [at('/charm-nest-bridge.js?v=20261028-x1'), {}, 'static'], [at('/vendor/jszip-3.10.1.min.js?v=20261028-sw1'), {}, 'static'], [at('/charm-nest-bridge.js'), {}, ''],
    [at('/charm-nest-1.html?v=1'), {}, ''], [at('/'), { mode: 'navigate' }, ''], [at('/charm-nest-bridge.js?v=1'), { method: 'POST' }, ''], ['https://other.app/charm-nest-bridge.js?v=1', {}, ''],
    [at('/order-timeline.js?v=1'), {}, 'static'], [at('/design-1.html?v=1'), {}, ''], [at('/.netlify/functions/charmNestLibrary?v=1'), {}, ''], [at('/vendor/fonts/emoji-sequences.json'), {}, ''],
    [pic('charmnest/sheets/s1/preview.png', '1761600000000'), {}, 'pic'], [pic('charmnest/sheets/s1/preview.png', ''), {}, ''], [pic('charmnest/sandbox/sheets/s1/preview.png', '1761600000000'), {}, ''],
    [pic('charmnest/master/ABC.ai', ''), {}, ''], [pic('charmnest/sheets/s1/preview.png', '1761600000000'), { range: true }, '']
  ];
  const bad = table.filter(([u, x, want]) => SW.classify(u, Object.assign({}, o, x)) !== want);
  ok(!bad.length, `classify: ${table.length} addresses (versioned files and pictures kept; pages, functions, the sandbox, masters, ranges, unversioned never)${bad.length ? ' — wrong: ' + bad.map(b => b[0]).join(', ') : ''}`);
  ok(SW.fileOf(at('/charm-nest-bridge.js?v=a')) === SW.fileOf(at('/charm-nest-bridge.js?v=b')) && SW.fileOf(pic('charmnest/sheets/s1/preview.png', '1761600000000')) === SW.fileOf(pic('charmnest/sheets/s1/preview.png', '1761600000999')), 'fileOf: two versions of a file are the same file');
  const e = SW.evict([{ url: 'a', b: 40, u: 1 }, { url: 'b', b: 40, u: 3 }, { url: 'c', b: 40, u: 2 }], { bytes: 70, n: 10 });
  ok(JSON.stringify(e) === '["a","c"]' && SW.evict([{ url: 'a', b: 1, u: 1 }], { bytes: 100, n: 10 }).length === 0, `evict: the least recently used go first, down to 90% of the cap (${JSON.stringify(e)})`);
}

const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome';
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules', '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));

// ── the server: bridge-server.cjs, with every request counted and the static files answered as Netlify answers them ──
const hits = [];   // { m, p (path + query), s (status) }
const tx = { html: null, sw: null, gone: false };   // the test's changes to what the server answers: tx.html(text) -> text; tx.sw(text) -> text: the worker's script (/charm-nest-sw.js, no query); tx.gone: the file missing (404, for the worker and the page's script both)
const RTT = +process.env.RTT || 0;   // RTT=<ms>: every request waits this long first (a network between the shop and the site; the numbers printed without it are localhost's)
const realCreate = http.createServer;
http.createServer = handler => realCreate((req, res) => RTT ? setTimeout(serve, RTT, handler, req, res) : serve(handler, req, res));
function serve(handler, req, res) {
  const u = new URL(req.url, 'http://x'), p = decodeURIComponent(u.pathname), rec = { m: req.method, p: u.pathname + u.search, s: 0 }; hits.push(rec);
  const end0 = res.end.bind(res), head0 = res.writeHead.bind(res);
  res.writeHead = (s, ...a) => { rec.s = s; return head0(s, ...a); };
  if (p.startsWith('/.netlify/') || p.startsWith('/__') || req.method !== 'GET') return handler(req, res);
  const f = path.join(root, p === '/' ? 'charm-nest-1.html' : p);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) return handler(req, res);
  const base = { 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp', 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' };
  if (path.basename(f) === 'charm-nest-sw.js' && tx.gone) { rec.s = 404; res.writeHead(404, base); return res.end(); }
  if (path.basename(f) === 'charm-nest-sw.js' && tx.sw && !u.search) {
    rec.s = 200; res.writeHead(200, Object.assign({ 'Content-Type': 'text/javascript', 'Cache-Control': 'max-age=0, no-cache, no-store, must-revalidate' }, base)); return res.end(tx.sw(fs.readFileSync(f, 'utf8')));
  }
  if (/\.html$/.test(f)) {   // the page: no-store, as netlify.toml has it (the bridge adds its sign-in seed; a test may change the text)
    res.setHeader('Cache-Control', 'max-age=0, no-cache, no-store, must-revalidate');
    if (tx.html) res.end = (b, ...a) => end0(b != null ? tx.html(String(b)) : b, ...a);
    return handler(req, res);
  }
  const st = fs.statSync(f), etag = `"${st.size.toString(16)}-${Math.floor(st.mtimeMs).toString(16)}"`;
  const cc = /^\/charm-nest-[^/]*\.js$/.test(p) ? 'max-age=0, no-cache, must-revalidate' : 'public, max-age=0, must-revalidate';
  if (req.headers['if-none-match'] === etag) { rec.s = 304; res.writeHead(304, Object.assign({ ETag: etag, 'Cache-Control': cc }, base)); return res.end(); }
  res.setHeader('ETag', etag); res.setHeader('Cache-Control', cc);
  return handler(req, res);
}
const { start } = require('./bridge-server.cjs');

const isStatic = h => !h.p.startsWith('/.netlify/') && !h.p.startsWith('/__');
const isFn = h => h.p.startsWith('/.netlify/functions/');
const short = p => p.replace(/\?v=[^&]*/, '?v=…');
const USABLE = () => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true && document.querySelector('#modeSeg button[data-mode="orders"]');

(async () => {
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); process.exit(failed ? 1 : 0); }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const rows = [];
  async function ctx(o = {}) {
    // (no context.route: Playwright turns the HTTP cache off while routing, and the baseline must revalidate as a browser does)
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.addInitScript(([s, sw]) => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); if (sw) localStorage.setItem('cn.sw', sw); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
      // the first frame the person can use: the page booted, the cloud answered, the tabs drawn (as tests/charm-nest/nav-instant.cjs waits for it)
      const tick = () => { try { if (window.CN && window.Orders && window.Review && CN.S.cloud.ok === true && document.querySelector('#modeSeg button[data-mode="orders"]')) { window.__usable = performance.now(); return; } } catch (_) {} requestAnimationFrame(tick); };
      requestAnimationFrame(tick);
    }, [{ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' }, o.sw || '']);
    const page = await context.newPage(); page.setDefaultTimeout(60000);
    const errors = [], resp = [];
    page.on('pageerror', e => errors.push(e.message));
    page.on('response', r => resp.push({ url: r.url(), sw: r.fromServiceWorker(), status: r.status(), doc: r.request().resourceType() === 'document' }));
    return { context, page, errors, resp };
  }
  /** One load (goto or reload), to the usable screen and a quiet 1.5 s after it: the requests that reached the server, the responses the page saw, the times. */
  async function load(w, name, how, extra) {
    const h0 = hits.length, r0 = w.resp.length;
    if (how === 'reload') await w.page.reload({ waitUntil: 'load' }); else await w.page.goto(srv.sorterOrigin + how, { waitUntil: 'load' });
    await w.page.waitForFunction(() => window.__usable > 0, null, { timeout: 60000 });
    const t = await w.page.evaluate(() => { const n = performance.getEntriesByType('navigation')[0], s = performance.getEntriesByType('resource').filter(e => /\.(js|css)(\?|$)/.test(e.name)); return { usable: Math.round(window.__usable), html: Math.round(n.responseEnd), dcl: Math.round(n.domContentLoadedEventEnd), scripts: Math.round(Math.max(0, ...s.map(e => e.responseEnd))), controlled: !!navigator.serviceWorker.controller }; });
    await w.page.waitForTimeout(1500);
    if (extra) await extra();
    const got = hits.slice(h0), st = got.filter(isStatic), fn = got.filter(isFn), seen = w.resp.slice(r0);
    const row = { name, static: st.length, s304: st.filter(h => h.s === 304).length, fns: fn.length, fromSW: seen.filter(r => r.sw).length, ...t, list: st.map(h => short(h.p)), got, seen, load: os.loadavg()[0].toFixed(2) };
    rows.push(row);
    console.log(`  · ${name.padEnd(38)} static requests ${String(row.static).padStart(3)} (${row.s304} × 304) · from the worker ${String(row.fromSW).padStart(3)} · functions ${String(row.fns).padStart(2)} · usable ${String(t.usable).padStart(5)} ms (page arrived ${t.html} ms, last script ${t.scripts} ms, usable ${t.usable - t.scripts} ms after it) · load ${row.load}`);
    return row;
  }
  const ours = page => page.evaluate(async () => ({ regs: (await navigator.serviceWorker.getRegistrations()).filter(r => /charm-nest-sw\.js/.test((r.active || r.waiting || r.installing || {}).scriptURL || '')).length, caches: (await caches.keys()).filter(k => /^cn-(static|pics|sw-meta)-/.test(k)) }));
  const until = async (page, fn, arg, ms = 20000) => { try { await page.waitForFunction(fn, arg, { timeout: ms, polling: 200 }); return true; } catch (_) { return false; } };
  const goneWorker = (page, ms) => until(page, async () => (await navigator.serviceWorker.getRegistrations()).every(r => !/charm-nest-sw\.js/.test((r.active || r.waiting || r.installing || {}).scriptURL || '')) && (await caches.keys()).every(k => !/^cn-(static|pics|sw-meta)-/.test(k)), null, ms);
  try {
    console.log(`  load ${os.loadavg().map(x => x.toFixed(2)).join(' ')} · Chromium ${browser.version()}`);
    // ── today: no worker (automation default) ──
    const a = await ctx();
    const a0 = await load(a, 'no worker, cold', '/charm-nest-1.html');
    const a1 = await load(a, 'no worker, warm reload', 'reload');
    const a2 = await load(a, 'no worker, warm reload again', 'reload');
    ok((await ours(a.page)).regs === 0 && !a.errors.length, `automation without cn.sw = "on": no worker registered, the page as today (${a.errors.length} page errors)`);
    await a.context.close();

    // ── with the worker ──
    const b = await ctx({ sw: 'on' });
    const b0 = await load(b, 'worker, cold (first visit)', '/charm-nest-1.html', async () => {
      await until(b.page, () => !!(navigator.serviceWorker.controller && window.CNServiceWorker && CNServiceWorker.adopted), null, 60000);
    });
    const st0 = await b.page.evaluate(() => CNServiceWorker.status());
    console.log(`    the worker ${st0.worker && st0.worker.version}: adopted ${CNServiceWorker_adopted(await b.page.evaluate(() => CNServiceWorker.adopted))} of the files the page loaded before it ran; kept ${st0.worker && st0.worker.static.n} files (${st0.worker && (st0.worker.static.bytes / 1048576).toFixed(1)} MB)`);
    ok(st0.controlled && st0.worker && st0.worker.static.n >= 100, `W0 the worker starts after the page is usable and keeps what the page loaded (controlled ${st0.controlled}, ${st0.worker && st0.worker.static.n} files kept)`);
    const b1 = await load(b, 'worker, warm reload', 'reload');
    const b2 = await load(b, 'worker, warm reload again', 'reload');
    ok(b1.static <= 3 && b2.static <= 3, `W1 a warm reload makes ≤ 3 static requests: ${b1.static} and ${b2.static} [${b1.list.join(' ')}] (without the worker: ${a1.static} and ${a2.static})`);
    ok(b1.controlled && b1.fromSW >= 100, `W1 the page's files came from the worker: ${b1.fromSW} responses`);
    // W3 the page is never answered by the worker: it reached the server at every load and its response says so
    const docs = [b0, b1, b2].map(r => ({ server: r.got.filter(h => h.m === 'GET' && /^\/charm-nest-1\.html/.test(h.p)).length, sw: r.seen.filter(x => x.doc && x.sw).length }));
    ok(docs.every(d => d.server === 1 && d.sw === 0), `W3 the page (HTML) came from the server at each load, never from the worker: ${JSON.stringify(docs)}`);
    // W4 functions: every call of the warm reloads reached the server; none was answered by the worker
    const fnSeen = [b1, b2].flatMap(r => r.seen.filter(x => /\/\.netlify\/functions\//.test(x.url)));
    ok(fnSeen.length > 0 && fnSeen.every(x => !x.sw) && [b1, b2].every(r => r.fns >= r.seen.filter(x => /\/\.netlify\/functions\//.test(x.url)).length), `W4 every function call of the warm reloads reached the server (${b1.fns + b2.fns} calls), none answered by the worker`);
    {
      const n0 = hits.filter(h => /authGate/.test(h.p)).length;
      const sw = await b.page.evaluate(async () => { const r1 = await fetch('/.netlify/functions/authGate'), r2 = await fetch('/.netlify/functions/authGate'); await r1.text(); await r2.text(); return [r1, r2].map(r => r.headers.get('content-type')); });
      ok(hits.filter(h => /authGate/.test(h.p)).length - n0 === 2, `W4 the same function GET twice reaches the server twice (${hits.filter(h => /authGate/.test(h.p)).length - n0}; ${sw.join(', ')})`);
    }
    // W4 pictures: a versioned production picture is read once; a sandbox picture and an unversioned one are never kept
    {
      const put = p => srv.st.blobs.set(p, { buf: Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/fteaysAAAAASUVORK5CYII=', 'base64'), generation: 7, meta: { contentType: 'image/png', metadata: {} } });
      const addr = (p, v) => '/.netlify/functions/charmNestAsset?url=' + encodeURIComponent(`https://firebasestorage.googleapis.com/v0/b/test-bucket/o/${encodeURIComponent(p)}?alt=media&token=t${v ? '&v=' + v : ''}`);
      const P = { prod: addr('charmnest/sheets/sw1/preview.png', '1761650000000'), sandbox: addr('charmnest/sandbox/sheets/sw1/preview.png', '1761650000000'), plain: addr('charmnest/sheets/sw2/preview.png', '') };
      put('charmnest/sheets/sw1/preview.png'); put('charmnest/sandbox/sheets/sw1/preview.png'); put('charmnest/sheets/sw2/preview.png');
      const reads = {};
      for (const [k, u] of Object.entries(P)) {
        const n0 = hits.filter(h => h.p === u).length;
        // (no-store: the HTTP cache is not asked, so only the worker can answer the second read)
        const st = await b.page.evaluate(async u => { const out = []; for (let i = 0; i < 2; i++) { const r = await fetch(u, { cache: 'no-store' }); out.push(r.status + ':' + (await r.arrayBuffer()).byteLength); await new Promise(r => setTimeout(r, 300)); } return out; }, u);
        reads[k] = hits.filter(h => h.p === u).length - n0 + ' (' + st.join(' ') + ')';
      }
      ok(/^1 /.test(reads.prod) && /^2 /.test(reads.sandbox) && /^2 /.test(reads.plain), `W4 pictures read twice reach the server: versioned production ${reads.prod} · sandbox ${reads.sandbox} · unversioned ${reads.plain}`);
    }
    // W2 one file's token changes: that file is fetched, nothing else, and its older copy is dropped
    {
      const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), m = /charm-nest-search\.js\?v=([^"]+)"/.exec(html);
      tx.html = s => s.replace('charm-nest-search.js?v=' + m[1] + '"', 'charm-nest-search.js?v=' + m[1] + '-swt1"');
      const b3 = await load(b, 'worker, one file has a new token', 'reload', () => b.page.waitForTimeout(2600));   // (the index is written 2 s after a change)
      const extra = b3.list.filter(p => !b1.list.includes(p) && !b2.list.includes(p));
      const kept = await b.page.evaluate(async () => (await (await caches.open('cn-static-v1')).keys()).map(r => r.url).filter(u => /charm-nest-search\.js/.test(u)).map(u => u.replace(/^.*\?v=/, '')));
      ok(extra.length === 1 && /charm-nest-search\.js/.test(extra[0]) && b3.got.some(h => h.p.endsWith('-swt1')) && b3.static <= b1.static + 1, `W2 a new token for one file fetches that file only: [${b3.list.join(' ')}]`);
      ok(kept.length === 1 && /-swt1$/.test(kept[0]), `W2 the older copy of that file is dropped from the cache: ${JSON.stringify(kept)}`);
      tx.html = null;
    }
    // W5 the page's switch: CN_SW = "off" in charm-nest-1.html unregisters the worker and deletes its caches; the page works
    {
      tx.html = s => s.replace('window.CN_SW="on"', 'window.CN_SW="off"');
      await load(b, 'worker switched off by the page', 'reload');
      const gone = await goneWorker(b.page);
      ok(gone && await b.page.evaluate(() => !!(window.CN && CN.S.cloud.ok)), `W5 CN_SW = "off": the worker unregistered and its caches deleted (${JSON.stringify(await ours(b.page))}), the page usable`);
      tx.html = null;
      const back = await load(b, 'switched on again (cold again)', 'reload', () => until(b.page, () => !!(navigator.serviceWorker.controller && CNServiceWorker.adopted), null, 60000));
      ok((await ours(b.page)).regs === 1 && back.static > 100, `W5 back on: the worker registers again; this load came from the network (${back.static} static requests)`);
      // ?sw=off for one computer, kept until ?sw=on
      await load(b, '?sw=off', '/charm-nest-1.html?sw=off');
      const off1 = await goneWorker(b.page), pref = await b.page.evaluate(() => localStorage.getItem('cn.sw'));
      await load(b, 'after ?sw=off, plain address', '/charm-nest-1.html');
      const still = await ours(b.page);
      ok(off1 && pref === 'off' && still.regs === 0 && !still.caches.length, `W5 ?sw=off: unregistered, caches deleted, and it stays off on this computer (cn.sw = ${pref}, ${JSON.stringify(still)})`);
      await load(b, '?sw=on', '/charm-nest-1.html?sw=on', () => until(b.page, () => !!(navigator.serviceWorker.controller && CNServiceWorker.adopted), null, 60000));
      ok((await ours(b.page)).regs === 1, '?sw=on: the worker is back');
    }
    // W5 the worker's own retirement: RETIRED = true reaches the computer at its next load (the browser reads the worker's script fresh), which deletes the caches and unregisters
    {
      tx.sw = s => s.replace('const RETIRED = false;', 'const RETIRED = true;');
      assert.ok(fs.readFileSync(path.join(root, 'charm-nest-sw.js'), 'utf8').includes('const RETIRED = false;'));
      await load(b, 'worker RETIRED on the server', 'reload', () => b.page.waitForTimeout(6000));   // (the page registers it again at idle: it removes itself at once)
      const gone = await goneWorker(b.page, 30000);
      ok(gone && await b.page.evaluate(() => !!(window.CN && CN.S.cloud.ok)), `W5 RETIRED = true: the worker removed itself and its caches (${JSON.stringify(await ours(b.page))}), the page usable`);
      tx.sw = null;
    }
    ok(!b.errors.length, `no page errors with the worker (${b.errors.slice(0, 3).join(' | ')})`);
    await b.context.close();

    // ── W6 no worker file, a worker that cannot install: the page boots as today ──
    for (const [name, how] of [['no worker file (404)', 'gone'], ['a worker that fails to install', () => 'throw new Error("broken worker");']]) {
      if (how === 'gone') tx.gone = true; else tx.sw = how;
      const c = await ctx({ sw: 'on' });
      const r = await load(c, name, '/charm-nest-1.html', () => c.page.waitForTimeout(3000));
      const s = await c.page.evaluate(async () => window.CNServiceWorker ? CNServiceWorker.status() : { registered: (await navigator.serviceWorker.getRegistrations()).length > 0, error: 'charm-nest-sw.js did not load' });
      ok(!c.errors.length && r.usable > 0 && !s.registered && /./.test(s.error), `W6 ${name}: the page is usable at ${r.usable} ms, no page error, no worker (${s.error.slice(0, 80)})`);
      await c.context.close(); tx.gone = false; tx.sw = null;
    }

    // ── the numbers ──
    console.log('\n  static requests per load and the time to the first usable screen (ms from the navigation; "after scripts": from the last script\'s arrival)');
    for (const r of [a0, a1, a2, b0, b1, b2]) console.log(`    ${r.name.padEnd(30)} ${String(r.static).padStart(4)} static (${String(r.s304).padStart(3)} × 304, ${String(r.fromSW).padStart(3)} from the worker)   usable ${String(r.usable).padStart(5)}   after scripts ${String(r.usable - r.scripts).padStart(5)}   dcl ${String(r.dcl).padStart(5)}`);
    console.log(`    load average ${os.loadavg().map(x => x.toFixed(2)).join(' ')}`);
  } finally { await browser.close(); srv.close(); http.createServer = realCreate; }
  console.log(failed ? `\nservice-worker: ${failed} check(s) FAILED` : '\nservice-worker: all checks passed');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
function CNServiceWorker_adopted(x) { return x && x.adopted != null ? x.adopted : JSON.stringify(x); }
