// Thumbnails that stay (Paul, 9 Oct 2026): "on every refresh, I have to go through the downloading of all the thumbnails ... I need
// fast loads while scrolling." The loading layer is charm-nest-thumbs.js (window.CharmNestThumbs); the Master tab, the Library's
// charm tiles and the SKU picker go through it, and a card is still drawn by the same code.
//   A · files: the module is built into the public site, loaded before the bridge, and the three call sites use it
//   B · the module, in Chromium under the site's COEP header: only the screen and a margin are asked for, the nearest first;
//       a fast scroll asks for nothing it passes; a refresh shows kept pictures with no request and no drawing; a changed
//       drawing (script address) or a re-indexed design (stamp) is asked for again; the store stays under its caps, drops what
//       an older drawing made and what nobody used for 45 days; no IndexedDB still works; a failed picture is not kept;
//       data-pic images are read once and kept; a plain cross-origin <img> is blocked by COEP (why the SKU picker goes through
//       the asset function)
//   C · the real Sorter page (bridge-server): the Master tab over 150 designs asks the asset function only for what it shows,
//       a refresh asks for nothing it already drew, a fast scroll does not queue what it passes, and the picture tags go through
//       the same-origin asset function
//   node tests/charm-nest/thumb-cache.cjs      (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome> for B and C; skipped without)
'use strict';
const fs = require('fs'), path = require('path'), http = require('http'), assert = require('assert/strict'), os = require('os');
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ── A · files ── */
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
ok(/"charm-nest-thumbs\.js"/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')), 'the module is in the public build list');
const at = s => html.indexOf(s);
ok(at('<script src="charm-nest-thumbs.js?v=') > at('<script src="charm-nest-assets.js') && at('<script src="charm-nest-thumbs.js?v=') < at('<script src="charm-nest-bridge.js'), 'the page loads the module after the asset reader and before the bridge, with a cache token');
ok(/CharmNestThumbs/.test(bridge) && /T\.mount\(grid/.test(bridge) && /T\.designKey\(geom\.aiPath/.test(bridge), 'the Master tab loads its pictures through the module, keyed by the design file');
ok(/Pool\.masterPreview\(entry,sizeOf\(entry\)\)/.test(bridge), 'and the picture is still drawn by Pool.masterPreview (the card is not redrawn another way)');
ok(/mountMasterPreviewsPlain/.test(bridge), 'a page without the module keeps the plain loader');
ok((html.match(/\$\{picTag\(c\.thumbUrl, /g) || []).length === 2 && /mountPics\(body\)/.test(html) && /mountPics\(\$\("#lsBody"\)\)/.test(html), 'the Library charm tiles and a sheet\'s charm grid use picTag and mountPics');
ok(/Master\.pictureTag\(Master\.entryFor\(k\)\)/.test(bridge) && /Master\.mountPictures\(c\)/.test(bridge) && !/<img alt="" src="\$\{esc\(Master\.thumbOf/.test(bridge), 'the SKU picker no longer puts the bare Storage address in an <img>');
const T0 = require('../../charm-nest-thumbs.js');
ok(T0.designKey('a.ai', 'h', 1) !== T0.designKey('a.ai', 'h', 2) && T0.designKey('a.ai', 'h', 1) !== T0.designKey('a.ai', 'g', 1) && T0.designKey('a.ai', 'h', 1) !== T0.designKey('b.ai', 'h', 1), 'a key names the file, its hash and its indexing time');
ok(T0.designKey('a.ai', 'h', 1) === T0.designKey('a.ai', 'h', 1) && /^m\|/.test(T0.designKey('a.ai')), 'and is the same each time');
ok(/^w\|\d+\|/.test(T0.weeklyKey('x')), 'a picture with no version of its own is kept for a week');

/* ── the browser ── */
let chromium = null;
for (const d of [process.env.PW_DIR, path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].filter(Boolean)) { try { ({ chromium } = require(path.join(d, 'playwright-core'))); break; } catch (_) { /* next */ } }
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!chromium || !fs.existsSync(CHROME)) { console.log(`thumb-cache: ${n} file checks OK · no playwright-core or chrome: the browser parts B and C were not run`); process.exit(0); }

const PNG1 = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/Z8fLAAAAABJRU5ErkJggg==', 'base64');
async function partB() {
  const hits = { pic: 0, bad: 0 }; let other = null;
  const otherSrv = http.createServer((q, r) => { r.writeHead(200, { 'Content-Type': 'image/png' }); r.end(PNG1); }).listen(0);
  const srv = http.createServer((q, r) => {
    const u = new URL(q.url, 'http://x'); const hdr = { 'Cross-Origin-Embedder-Policy': 'require-corp', 'Cross-Origin-Opener-Policy': 'same-origin' };
    if (u.pathname === '/charm-nest-thumbs.js') { r.writeHead(200, Object.assign({ 'Content-Type': 'text/javascript' }, hdr)); return r.end(fs.readFileSync(path.join(root, 'charm-nest-thumbs.js'))); }
    if (u.pathname === '/charm-nest-pdf.js') { r.writeHead(200, Object.assign({ 'Content-Type': 'text/javascript' }, hdr)); return r.end('/* stands for the drawing code: only its address matters */'); }
    if (u.pathname.startsWith('/pic/')) { hits.pic++; r.writeHead(200, Object.assign({ 'Content-Type': 'image/png', 'Cache-Control': 'private, no-cache', 'Cross-Origin-Resource-Policy': 'same-origin' }, hdr)); return r.end(PNG1); }
    if (u.pathname.startsWith('/bad/')) { hits.bad++; r.writeHead(404, hdr); return r.end('no'); }
    if (u.pathname === '/t.html') {
      r.writeHead(200, Object.assign({ 'Content-Type': 'text/html', 'Cache-Control': 'no-store' }, hdr));
      return r.end(`<!doctype html><meta charset=utf-8><body style="margin:0"><script src="/charm-nest-pdf.js?v=${u.searchParams.get('v') || '1'}"></script><script src="/charm-nest-thumbs.js"></script>
<div id=sc style="height:600px;overflow:auto"><div id=grid style="display:grid;grid-template-columns:repeat(6,1fr);gap:8px"></div></div>
<script>
const T = window.CharmNestThumbs, sc = document.getElementById('sc'), grid = document.getElementById('grid');
window.log = []; window.mounts = [];
const colour = i => { const c = document.createElement('canvas'); c.width = c.height = 8; const x = c.getContext('2d'); x.fillStyle = 'hsl(' + (i * 37 % 360) + ',70%,50%)'; x.fillRect(0, 0, 8, 8); return c.toDataURL('image/png'); };
window.build = (count, o) => {
  o = o || {}; grid.innerHTML = ''; for (let i = 0; i < count; i++) { const d = document.createElement('div'); d.className = 't'; d.dataset.i = i; d.style.height = '150px'; d.textContent = 'Loading preview…'; grid.appendChild(d); }
  const m = T.mount(grid, { selector: '.t', concurrency: o.concurrency || 6, keyOf: t => T.designKey('p/' + t.dataset.i, 'h', o.stamp || 1),
    produce: async t => { window.log.push({ i: +t.dataset.i, at: performance.now(), top: sc.scrollTop }); await new Promise(r => setTimeout(r, o.delay || 25)); if (o.failFor === +t.dataset.i) throw new Error('boom'); return colour(+t.dataset.i); },
    paint: (t, u) => { t.innerHTML = '<img src="' + u + '">'; t.dataset.done = '1'; }, fail: t => { t.textContent = 'x'; t.dataset.failed = '1'; } });
  window.mounts.push(m); return m;
};
window.done = () => [...document.querySelectorAll('.t[data-done]')].map(t => +t.dataset.i);
window.visibleRange = () => { const r = sc.getBoundingClientRect(), out = []; document.querySelectorAll('.t').forEach(t => { const b = t.getBoundingClientRect(); if (b.bottom > r.top && b.top < r.bottom) out.push(+t.dataset.i); }); return out; };
</script>`);
    }
    if (u.pathname === '/plain.html') { r.writeHead(200, Object.assign({ 'Content-Type': 'text/html' }, hdr)); return r.end(`<img id=a src="http://127.0.0.1:${otherSrv.address().port}/x.png">`); }
    r.writeHead(404); r.end();
  }).listen(0);
  await new Promise(r => srv.once('listening', r)); const base = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  try {
    const stable = async (page, ms = 700, max = 60000) => { const t0 = Date.now(); let last = -1, since = Date.now(); for (;;) { const c = await page.evaluate(() => window.log.length + ':' + window.done().length); if (c === last) { if (Date.now() - since >= ms) return; } else { last = c; since = Date.now(); } if (Date.now() - t0 > max) throw new Error('did not settle'); await sleep(60); } };
    const ctx = await browser.newContext({ viewport: { width: 1000, height: 700 } }), page = await ctx.newPage(), errs = [];
    page.on('pageerror', e => errs.push(e.message));

    // 1 · first visit: the screen and a margin, the nearest first
    await page.goto(base + '/t.html?v=1'); await page.evaluate(() => window.build(400));
    await stable(page);
    const visible = await page.evaluate(() => window.visibleRange()), log1 = await page.evaluate(() => window.log.map(x => x.i)), done1 = await page.evaluate(() => window.done());
    ok(visible.length >= 18 && visible.length <= 30, 'about 22 tiles on screen (' + visible.length + ')');
    ok(visible.every(i => done1.includes(i)), 'every tile on screen has its picture');
    ok(Math.max(...log1) < 70 && log1.length >= visible.length && log1.length <= 60, 'only the screen and a margin were asked for (' + log1.length + ' of 400, the farthest is ' + Math.max(...log1) + ')');
    ok(log1.slice(0, 6).every(i => i < 12), 'the first asked for are the top ones on screen: ' + log1.slice(0, 6));
    ok(!(await page.evaluate(() => window.done().includes(399))), 'a tile far below is not drawn');

    // 2 · a fast scroll down: nothing it passes is asked for, and the tiles it stops at come first
    const before = log1.length;
    await page.evaluate(async () => { const sc = document.getElementById('sc'); for (let i = 0; i < 30; i++) { sc.scrollTop += 380; await new Promise(r => setTimeout(r, 25)); } window.scrollEndAt = performance.now(); });
    await stable(page);
    const log2 = await page.evaluate(() => window.log.slice().map(x => ({ i: x.i, at: x.at }))), scrollEnd = await page.evaluate(() => window.scrollEndAt), vis2 = await page.evaluate(() => window.visibleRange());
    const passed = log2.slice(before).filter(x => x.i >= 70 && x.i < Math.min(...vis2) - 18);
    ok(passed.length <= 20, 'tiles scrolled past were (nearly) never asked for: ' + passed.length + ' of the ' + (Math.min(...vis2) - 88) + ' passed');
    ok((await page.evaluate(() => window.done())).filter(i => vis2.includes(i)).length === vis2.length, 'the tiles it stopped at all have their pictures');
    const after = log2.filter(x => x.at > scrollEnd).slice(0, 6);
    ok(after.length === 6 && after.every(x => x.i >= Math.min(...vis2) - 6 && x.i <= Math.max(...vis2) + 6), 'after the scroll stopped, the first asked for are the tiles on screen: ' + after.map(x => x.i));
    const total1 = log2.length;
    const st1 = await page.evaluate(() => window.CharmNestThumbs.stats());
    ok(st1.disk && st1.entries === total1 && st1.bytes > 0, 'every picture drawn is kept on disk (' + st1.entries + ' = ' + total1 + ')');

    // 3 · a refresh: the pictures seen come back with no drawing and no request
    await page.goto(base + '/t.html?v=1'); await page.evaluate(() => window.build(400));
    await stable(page);
    const log3 = await page.evaluate(() => window.log.map(x => x.i)), done3 = await page.evaluate(() => window.done());
    ok(log3.length === 0, 'after a refresh nothing seen before is drawn again: ' + log3.length + ' asked for');
    ok(done3.length >= 18 && done3.every(i => i < 70), 'and the first screen is there from the disk copy (' + done3.length + ' tiles)');
    const rest = await page.evaluate(async () => { const sc = document.getElementById('sc'); sc.scrollTop = 10000; await new Promise(r => setTimeout(r, 600)); return { shown: window.visibleRange(), done: window.done().length, asked: window.log.length }; });
    ok(rest.asked <= 40, 'only what was never seen is asked for after the refresh (' + rest.asked + ')');

    // 4 · a changed drawing (another address of the drawing code) and a re-indexed design (another stamp) are asked for again
    await page.goto(base + '/t.html?v=2'); await page.evaluate(() => window.build(400));
    await stable(page);
    const log4 = await page.evaluate(() => window.log.map(x => x.i));
    ok(log4.length >= 18 && log4.slice(0, 6).every(i => i < 12), 'a changed drawing draws the tiles again, not the old pictures: ' + log4.length);
    const tr = await page.evaluate(() => window.CharmNestThumbs.trim(true).then(() => window.CharmNestThumbs.stats()));
    ok(tr.entries === log4.length, 'and the pictures of the older drawing are removed (' + tr.entries + ' kept, ' + log4.length + ' of the new drawing)');
    await page.evaluate(() => { window.mounts.forEach(m => m.stop()); window.CharmNestThumbs._forget(); window.build(60, { stamp: 2 }); });
    await stable(page);
    ok((await page.evaluate(() => window.log.length)) > log4.length, 'a re-indexed design (a new stamp) is drawn again');
    ok(!errs.length, 'no page error: ' + errs.join(' | '));

    // 5 · the caps: entries, bytes, the 45 days, and the oldest-used go first
    await page.evaluate(() => window.CharmNestThumbs.clear());
    const cap = await page.evaluate(async () => {
      const T = window.CharmNestThumbs, one = i => 'data:image/png;base64,' + btoa(String.fromCharCode(...new Uint8Array(1000).map((_, k) => (k + i) % 251)));
      T.LIMITS.entries = 40; T.LIMITS.bytes = 1e9;
      for (let i = 0; i < 100; i++) { await T.make('m|' + T.drawingVersion() + '|cap' + i + '||', async () => one(i)); await new Promise(r => setTimeout(r, 2)); }
      await new Promise(r => setTimeout(r, 300)); const a = await T.stats(); await T.trim(true); const b = await T.stats(); T._forget();
      const newest = await T.lookup('m|' + T.drawingVersion() + '|cap99||'), oldest = await T.lookup('m|' + T.drawingVersion() + '|cap0||');
      return { a, b, newest: !!newest, oldest: !!oldest };
    });
    ok(cap.b.entries <= 34 && cap.b.entries >= 30, 'beyond the entry cap the store is trimmed to 85% (' + cap.a.entries + ' -> ' + cap.b.entries + ')');
    ok(cap.newest && !cap.oldest, 'the newest picture stays and the oldest-used goes');
    const cap2 = await page.evaluate(async () => {
      const T = window.CharmNestThumbs; T._resetVersion(); T.LIMITS.entries = 9000; T.LIMITS.bytes = 20 * 1000; await T.trim(true); const a = await T.stats(); T.LIMITS.bytes = 120 * 1024 * 1024; return a;
    });
    ok(cap2.bytes <= 20 * 1000 * 0.85 + 1500 && cap2.entries >= 1, 'beyond the byte cap too (' + cap2.bytes + ' bytes)');
    const idle = await page.evaluate(async () => {
      const T = window.CharmNestThumbs; T._resetVersion(); const key = 'm|' + T.drawingVersion() + '|idle||', keep = 'm|' + T.drawingVersion() + '|kept||';
      const db = await new Promise(r => { const q = indexedDB.open('cn-thumbs', 1); q.onsuccess = () => r(q.result); });
      await T.make(key, async () => 'data:image/png;base64,AAAA'); await T.make(keep, async () => 'data:image/png;base64,AAAA'); await new Promise(r => setTimeout(r, 200));
      await new Promise(r => { const tx = db.transaction('idx', 'readwrite'), s = tx.objectStore('idx'), g = s.get(key); g.onsuccess = () => { const v = g.result; v.at = Date.now() - 46 * 86400000; s.put(v); }; tx.oncomplete = r; });
      db.close(); await T.trim(true); T._forget();
      return { gone: !(await T.lookup(key)), kept: !!(await T.lookup(keep)) };
    });
    ok(idle.gone && idle.kept, 'a picture unused for 45 days is dropped, one used today stays');

    // 6 · no IndexedDB (a private window): the page works from memory
    const ctx2 = await browser.newContext({ viewport: { width: 1000, height: 700 } }); await ctx2.addInitScript(() => { try { Object.defineProperty(window, 'indexedDB', { value: undefined }); } catch (_) {} });
    const p2 = await ctx2.newPage(); await p2.goto(base + '/t.html?v=1'); await p2.evaluate(() => window.build(100)); await stable(p2);
    const m1 = await p2.evaluate(() => ({ log: window.log.length, done: window.done().length, st: null }));
    ok(m1.done >= 18 && m1.log >= 18, 'without IndexedDB the tiles still load (' + m1.done + ')');
    await p2.evaluate(() => { window.mounts.forEach(m => m.stop()); window.build(100); });
    ok((await p2.evaluate(() => window.done().length)) === m1.done && (await p2.evaluate(() => window.log.length)) === m1.log, 'and a redrawn grid shows them from memory at once, with nothing asked for');
    ok(!(await p2.evaluate(() => window.CharmNestThumbs.stats())).disk, 'the store says there is no disk');
    await ctx2.close();

    // 7 · a failed picture says so, is not kept, and does not stop the others; it is tried again next time
    const ctx3 = await browser.newContext({ viewport: { width: 1000, height: 700 } }), p3 = await ctx3.newPage(); await p3.goto(base + '/t.html?v=1');
    await p3.evaluate(() => window.build(40, { failFor: 3 })); await stable(p3);
    ok((await p3.evaluate(() => document.querySelector('.t[data-i="3"]').dataset.failed)) === '1' && (await p3.evaluate(() => window.done().length)) >= 17, 'one failed picture is marked, the rest are drawn');
    await p3.evaluate(() => { window.mounts.forEach(m => m.stop()); window.CharmNestThumbs._forget(); window.log.length = 0; window.build(40); }); await stable(p3);
    ok((await p3.evaluate(() => window.log.map(x => x.i))).join() === '3', 'the failed one is the only one drawn again');

    // 8 · images with data-pic: read once, kept, and the address itself when it cannot be read
    await p3.evaluate(() => { const h = document.createElement('div'); h.id = 'imgs'; h.innerHTML = '<img id="i1" data-pic="/pic/1.png" data-pic-key="' + window.CharmNestThumbs.weeklyKey('one') + '"><img id="i2" data-pic="/pic/2.png" data-pic-key="' + window.CharmNestThumbs.weeklyKey('two') + '"><img id="i3" data-pic="/bad/3.png" data-pic-key="' + window.CharmNestThumbs.weeklyKey('three') + '">'; document.body.appendChild(h); window.CharmNestThumbs.mountImages(h); });
    await p3.waitForFunction(() => document.getElementById('i1').src.startsWith('data:') && document.getElementById('i2').src.startsWith('data:'));
    await p3.waitForFunction(() => /\/bad\/3\.png$/.test(document.getElementById('i3').src));
    ok(hits.pic === 2 && hits.bad >= 1, 'each image was read once (' + hits.pic + ' reads) and an unreadable one falls back to its own address');
    await p3.goto(base + '/t.html?v=1');
    await p3.evaluate(() => { const h = document.createElement('div'); h.innerHTML = '<img id="i1" data-pic="/pic/1.png" data-pic-key="' + window.CharmNestThumbs.weeklyKey('one') + '"><img id="i2" data-pic="/pic/2.png" data-pic-key="' + window.CharmNestThumbs.weeklyKey('two') + '">'; document.body.appendChild(h); window.CharmNestThumbs.mountImages(h); });
    await p3.waitForFunction(() => document.getElementById('i1').src.startsWith('data:') && document.getElementById('i2').src.startsWith('data:'));
    ok(hits.pic === 2, 'after a refresh they come from the kept copy with no request (' + hits.pic + ' reads in all)');
    await ctx3.close();

    // 9 · the site's COEP blocks a plain cross-origin image: why the SKU picker no longer puts the Storage address in an <img>
    const p4 = await ctx.newPage(); await p4.goto(base + '/plain.html'); await sleep(800);
    ok((await p4.evaluate(() => document.getElementById('a').naturalWidth)) === 0, 'a plain cross-origin <img> is blocked under require-corp');
    await ctx.close();
  } finally { await browser.close(); srv.close(); otherSrv.close(); }
}

/* ── C · the real Sorter page over a fake backend ── */
async function partC() {
  const { start } = require('./bridge-server.cjs'), { buildMaster } = require('./fixture-master.cjs');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-thumb-')), mp = path.join(tmp, 'THUMB-master.ai');
  await buildMaster(mp, { count: 8, edge: false });
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: '' }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const b = st.blobs.get(m ? decodeURIComponent(m[1]) : ''); if (!b) return r.fulfill({ status: 404, body: 'no blob' }); r.fulfill({ status: 200, contentType: b.meta.contentType || 'application/octet-stream', body: b.buf }); });
    await ctx.addInitScript(() => { window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} });
    const page = await ctx.newPage(), errs = [], asked = [];
    page.on('pageerror', e => errs.push(e.message));
    page.on('request', r => { if (/charmNestAsset/.test(r.url())) { const d = decodeURIComponent(decodeURIComponent(r.url())), m = /charmnest\/master\/([^?&]+)/.exec(d); asked.push(m ? m[1] : d); } });
    const boot = async () => { await page.goto(sorterOrigin + '/charm-nest-1.html'); await page.waitForFunction(() => window.CN && window.CN.S && CN.S.cloud.ok !== null); };
    await boot(); await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mFile', { state: 'attached' }); await page.setInputFiles('#mFile', mp);
    await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 180000 });
    // 150 designs: the 8 indexed ones cloned under other SKUs (their own file paths)
    const docs = [...st.docs.entries()].filter(([k]) => k.startsWith('Charm_Master_Index/')), base = 'https://firebasestorage.googleapis.com/v0/b/test-bucket/o/';
    const url = p => base + encodeURIComponent(p) + '?alt=media&token=t';
    for (let i = 0; i < 150; i++) {
      const [, d] = docs[i % docs.length], sku = 'ZT' + String(i).padStart(3, '0'), aiPath = `charmnest/master/${sku}.ai`, pngPath = `charmnest/master/${sku}.png`;
      st.blobs.set(aiPath, Object.assign({}, st.blobs.get(d.aiPath), { generation: 5000 + i })); st.blobs.set(pngPath, Object.assign({}, st.blobs.get(d.thumbPath), { generation: 5000 + i }));
      st.docs.set('Charm_Master_Index/' + sku, Object.assign({}, d, { sku, aiPath, thumbPath: pngPath, aiUrl: url(aiPath), thumbUrl: url(pngPath), indexedAt: 1791500000000 + i }));
    }
    for (const [k] of docs) st.docs.delete(k);
    const view = () => page.evaluate(() => { const g = document.querySelector('#mGrid'), vh = innerHeight, tiles = [...g.querySelectorAll('.skuTile')], vis = tiles.filter(t => { const r = t.getBoundingClientRect(); return r.bottom > 0 && r.top < vh; }); return { total: tiles.length, vis: vis.length, painted: vis.filter(t => t.querySelector('[data-preview-sku] img')).length, all: tiles.filter(t => t.querySelector('[data-preview-sku] img')).length }; });
    const painted = async (what) => { for (let i = 0; i < 400; i++) { const v = await view(); if (v.vis && v.painted === v.vis) return v; await sleep(150); } throw new Error('tiles not drawn: ' + what + ' ' + JSON.stringify(await view())); };
    const openMaster = async () => { await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mGrid .skuTile', { timeout: 60000 }); };

    // first visit (nothing kept)
    await boot(); await page.evaluate(() => window.CharmNestThumbs && CharmNestThumbs.clear()); await boot(); asked.length = 0;
    await openMaster(); const v1 = await painted('first visit');
    await sleep(800);
    const first = asked.filter(x => /\.ai$/.test(x));
    ok(v1.total === 150 && v1.vis >= 10, 'the Master tab draws the tiles on screen (' + v1.vis + ' of ' + v1.total + ')');
    ok(first.length >= v1.vis && first.length <= v1.vis * 3 + 10, 'first visit: only the screen and a margin asked the asset function (' + first.length + ' files for ' + v1.vis + ' on screen, of 150)');
    ok(!asked.some(x => /\.png$/.test(x)), 'the stored PNGs are not what the tiles are drawn from (the live drawing is), so none is fetched');
    // a fast scroll
    const n0 = asked.length;
    await page.evaluate(async () => { let sc = document.querySelector('#mGrid'); while (sc && !(sc.scrollHeight > sc.clientHeight + 5 && /auto|scroll/.test(getComputedStyle(sc).overflowY))) sc = sc.parentElement; sc = sc || document.scrollingElement; for (let i = 0; i < 40; i++) { sc.scrollBy(0, 300); await new Promise(r => setTimeout(r, 25)); } });
    const v2 = await painted('after the scroll');
    await sleep(600);
    const scrolled = [...new Set(asked.slice(n0).filter(x => /\.ai$/.test(x)))];
    ok(v2.all >= v2.vis, 'after a fast scroll the tiles it stopped at are drawn (' + v2.vis + ')');
    ok(scrolled.length <= 110, 'a fast scroll over 12,000 px did not queue every tile it passed (' + scrolled.length + ' of the 150 asked for)');
    // a refresh: what was drawn is not asked for again
    await boot(); asked.length = 0; await openMaster();
    const t0 = Date.now(); const v3 = await painted('refresh'); const ms = Date.now() - t0;
    await sleep(600);
    const again = asked.filter(x => /\.ai$/.test(x));
    ok(again.length <= 4, 'after a refresh the first screen asks the asset function for nothing it drew before (' + again.length + ' files, ' + ms + ' ms to draw ' + v3.vis + ' tiles)');
    // the pictures of other places go through the same-origin asset function
    const tags = await page.evaluate(() => { const e = [...B.master.entries.values()][0]; return { sku: Master.pictureTag(e), lib: picTag('https://firebasestorage.googleapis.com/v0/b/test-bucket/o/charmnest%2Fcharms%2Fabc.png?alt=media&token=t', 'abc') }; });
    ok(/crossorigin="anonymous" data-pic="\/\.netlify\/functions\/charmNestAsset\?url=/.test(tags.sku) && !/ src="/.test(tags.sku), 'the SKU picker tag goes through the asset function and is filled by the loader');
    ok(/data-pic="\/\.netlify\/functions\/charmNestAsset\?url=/.test(tags.lib) && /data-pic-key="w\|\d+\|c\|abc"/.test(tags.lib), 'the Library tile tag too, keyed by the charm for a week');
    ok(!errs.length, 'no page error: ' + errs.slice(0, 3).join(' | '));
    await ctx.close();
  } finally { await browser.close(); try { srv.close(); } catch (_) { /* done */ } }
}

(async () => {
  await partB();
  await partC();
  console.log(`thumb-cache OK · ${n} checks`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
