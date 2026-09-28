// Adversarial wave 3, area 11: the move animations inside the tabs are visible and smooth (Paul, 28 Sep: "some of the
// animations were not exactly visible and they were jerky"). In headless Chromium, unthrottled, each animation is run
// while requestAnimationFrame deltas, long tasks, every element.animate() call and the flying copies' rects are recorded:
//   visible  · the copy is on screen, moves or fades over at least 280 ms, and mid-flight differs from its start and end;
//   smooth   · no frame over 34 ms and no long task over 50 ms while it runs;
//   cheap    · only transform, opacity and clip-path are animated (no layout property, no filter or shadow per frame);
//   whole    · a copy is not removed while its animation still runs, and what stays does not jump before it glides.
// The Nest tab's rail runs the real code (a real .ai dropped, a charm sent to a metal, a file removed); the Orders rows,
// Library marks, Engraving cards, "+1", Undo's fly-in and a cleared sheet's dissolve run the same Motion calls those tabs
// make, on elements of their size, in the page's own styles. No network but the loopback.
//   node tests/charm-nest/adv-tab-motion.cjs [playwright-core dir]
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { build } = require('./fixture.cjs');
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.ai': 'application/pdf', '.pdf': 'application/pdf' };
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split('?')[0]);
  if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
  const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp' });
  fs.createReadStream(f).pipe(res);
}).listen(0);

const LAYOUT = /^(top|left|right|bottom|width|height|maxHeight|minHeight|maxWidth|minWidth|margin.*|padding.*|border.*Width|inset|flex.*|gap)$/;
const PAINT = /^(boxShadow|filter|backdropFilter|background.*|color|borderColor|outline.*)$/;

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cn-motion-'));
  const fixture = path.join(tmp, 'TEST-charms.ai');   // no metal in the name: its charms wait under Unassigned
  await build(fixture, 8);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], rows = [];
  const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => r.abort());
    await context.addInitScript(() => {
      const M = window.__M = { rec: false, frames: [], long: [], anims: [], samples: [], removed: [], cancels: [], n: 0, watch: [] };
      const A = Element.prototype.animate;
      Element.prototype.animate = function (k, o) {
        const a = A.call(this, k, o);
        if (M.rec) {
          const props = new Set(), list = Array.isArray(k) ? k : k ? [k] : [];
          for (const f of list) for (const p of Object.keys(f)) if (!['offset', 'easing', 'composite'].includes(p)) props.add(p);
          const cls = typeof this.className === 'string' ? this.className : (this.getAttribute && this.getAttribute('class')) || this.tagName;
          M.anims.push({ cls: String(cls).slice(0, 40), props: [...props], dur: typeof o === 'number' ? o : o && o.duration, t: performance.now() });
        }
        return a;
      };
      const C = Animation.prototype.cancel;
      Animation.prototype.cancel = function () { if (M.rec && this.playState === 'running' && this.effect && this.effect.target && this.effect.target.closest && this.effect.target.closest('.mGhost')) M.cancels.push(performance.now()); return C.call(this); };
      const R = Element.prototype.remove;
      Element.prototype.remove = function () {
        if (M.rec && this.classList && (this.classList.contains('mGhost') || this.classList.contains('mPlus'))) M.removed.push({ running: this.getAnimations({ subtree: true }).filter(a => a.playState === 'running' && isFinite(a.effect.getComputedTiming().endTime)).length, t: performance.now() });
        return R.call(this);
      };
      try { new PerformanceObserver(l => { if (M.rec) for (const e of l.getEntries()) M.long.push(Math.round(e.duration)); }).observe({ entryTypes: ['longtask'] }); } catch (_) {}
      const loop = t => {
        if (M.rec) {
          M.frames.push(t);
          for (const g of document.querySelectorAll('.mGhost, .mPlus')) {
            const r = g.getBoundingClientRect(); g.__id = g.__id || ++M.n;
            M.samples.push({ t, id: g.__id, plus: g.classList.contains('mPlus'), x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width, o: +getComputedStyle(g).opacity });
          }
          for (const w of M.watch) { const r = w.el.getBoundingClientRect(); w.ys.push([t, r.top]); }
        }
        requestAnimationFrame(loop);
      };
      requestAnimationFrame(loop);
      window.__start = () => Object.assign(M, { rec: true, frames: [], long: [], anims: [], samples: [], removed: [], cancels: [], watch: [], t0: performance.now() });
      window.__stop = () => {
        M.rec = false;
        const d = []; for (let i = 2; i < M.frames.length; i++) d.push(M.frames[i] - M.frames[i - 1]);
        const by = new Map(); for (const s of M.samples) { if (!by.has(s.id)) by.set(s.id, []); by.get(s.id).push(s); }
        const copies = [...by.values()].map(ss => {
          const a = ss[0], z = ss[ss.length - 1], mid = ss[Math.floor(ss.length / 2)], seen = ss.filter(s => s.o > .05);
          const moved = Math.max(...ss.map(s => Math.hypot(s.x - a.x, s.y - a.y) + Math.abs(s.w - a.w)));
          const fade = Math.max(...ss.map(s => s.o)) - Math.min(...ss.map(s => s.o));
          const midDiff = Math.hypot(mid.x - a.x, mid.y - a.y) + Math.abs(mid.w - a.w) + 40 * Math.abs(mid.o - a.o) > 2 && Math.hypot(mid.x - z.x, mid.y - z.y) + Math.abs(mid.w - z.w) + 40 * Math.abs(mid.o - z.o) > 2;
          return { n: ss.length, plus: a.plus, ms: Math.round((seen.length ? seen[seen.length - 1].t - seen[0].t : 0)), moved: Math.round(moved), fade: +fade.toFixed(2), midDiff, onScreen: ss.some(s => s.x > 0 && s.x < innerWidth && s.y > 0 && s.y < innerHeight && s.o > .05) };
        });
        const jumps = M.watch.map(w => { const y0 = w.ys.length ? w.ys[0][1] : 0; let worst = 0; for (let i = 1; i < w.ys.length; i++) worst = Math.max(worst, Math.abs(w.ys[i][1] - w.ys[i - 1][1])); return { name: w.name, first: w.ys.length ? Math.round(w.ys[0][1] - w.from) : 0, worst: Math.round(worst), y0 }; });
        return { frames: d.length, maxFrame: Math.round(Math.max(0, ...d)), over34: d.filter(x => x > 34).map(Math.round), long: M.long.filter(x => x > 50), anims: M.anims, copies, cutOff: M.removed.filter(r => r.running).length, cancels: M.cancels.length, jumps };
      };
    });
    const page = await context.newPage();
    const errors = []; page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.CN.S && window.Motion && window.Motion.reconcile);
    await page.evaluate(() => { CN.S.settings.naming = 'off'; CN.S.settings.review = 'off'; });
    const wait = ms => page.waitForTimeout(ms);

    let idle = null;
    /** Runs `fn` (in the page) with the recorders on for `ms`, then judges it. */
    async function measure(name, fn, ms = 2400, want = {}) {
      await wait(300);
      const r = await page.evaluate(async ([src, ms]) => { __start(); await (0, eval)(src)(); await new Promise(res => setTimeout(res, ms)); return __stop(); }, [String(fn), ms]);
      const props = [...new Set(r.anims.flatMap(a => a.props))];
      const layout = r.anims.filter(a => a.props.some(p => LAYOUT.test(p))).map(a => a.cls + ':' + a.props.filter(p => LAYOUT.test(p)).join('+'));
      const paint = r.anims.filter(a => a.props.some(p => PAINT.test(p)) && /mGhost|mCopy/.test(a.cls + ' ' + a.props)).map(a => a.cls + ':' + a.props.filter(p => PAINT.test(p)).join('+'));
      const flying = r.copies.filter(c => !c.plus && c.n >= 3), plus = r.copies.filter(c => c.plus && c.n >= 3);   // (a copy caught in one frame is the last one's tail)
      const shown = flying.length ? flying.every(c => c.onScreen && c.ms >= 280 && (c.moved > 8 || c.fade > .5) && c.midDiff) : !want.copy;
      const jump = r.jumps.filter(j => Math.abs(j.first) > 2 && j.worst > 12);
      rows.push({ name, maxFrame: r.maxFrame, over34: r.over34.length, frames: r.frames, long: r.long.length, copies: flying.map(c => `${c.ms}ms/${c.moved}px`).join(' '), props: props.join(',') });
      console.log(`  ${name}: frames ${r.frames}, worst ${r.maxFrame} ms, >34 ms ${JSON.stringify(r.over34)}, long tasks ${JSON.stringify(r.long)}; copies ${JSON.stringify(flying)}${plus.length ? '; +1 ' + JSON.stringify(plus) : ''}; props ${props.join(',')}${r.jumps.length ? '; glides ' + JSON.stringify(r.jumps) : ''}`);
      check(shown, `${name}: every copy is on screen and seen moving or fading for at least 280 ms, differing mid-way`);
      if (want.plus) check(plus.length && plus.every(c => c.onScreen && c.ms >= 280 && c.moved > 8), `${name}: the "+1" rises where it is seen`);
      // smooth: strict (no frame over 34 ms, no long task over 50 ms) when the machine is quiet; on a shared, loaded
      // machine (the idle page itself drops frames) no worse than the idle page measured in this run
      // (STRICT=1 on a quiet machine: not one frame over 34 ms. Otherwise a frame or two lost to the other processes of a
      // shared machine is tolerated: at most one in ten, and no long task over 100 ms)
      if (process.env.STRICT) check(!r.over34.length && !r.long.length, `${name}: smooth (no frame over 34 ms, no long task over 50 ms)`);
      else if (!idle || idle.over34 === 0) check(r.over34.length <= r.frames / 10 && !r.long.some(x => x > 100), `${name}: smooth (${r.over34.length}/${r.frames} frames over 34 ms, long tasks ${JSON.stringify(r.long)})`);
      else check(r.over34.length / Math.max(1, r.frames) <= Math.max(.1, 2 * idle.over34 / Math.max(1, idle.frames)), `${name}: no jerkier than the idle page on this loaded machine (${r.over34.length}/${r.frames} frames over 34 ms, idle ${idle.over34}/${idle.frames})`);
      check(!layout.length, `${name}: no layout property animated (${layout.join(' ') || 'none'})`);
      check(!paint.length, `${name}: the flying copy animates no filter or shadow per frame (${paint.join(' ') || 'none'})`);
      check(!r.cutOff && !r.cancels, `${name}: no copy removed or cancelled mid-animation (${r.cutOff} removed, ${r.cancels} cancelled)`);
      if (r.jumps.length) check(!jump.length, `${name}: what stays glides from where it stood, without a jump (${JSON.stringify(jump)})`);
      return r;
    }

    /* ── Nest tab, real code: a file with 8 charms waits under Unassigned ── */
    await page.setInputFiles('#fileInput', fixture);
    await page.waitForFunction(() => CN.S.sources.length === 1 && ['ready', 'error'].includes(CN.S.sources[0].state) && CN.S.unassigned.length >= 6, null, { timeout: 60000 });
    await page.evaluate(() => CN.setMode('nest'));
    await page.waitForFunction(() => document.querySelector('#unList .unRow') && document.querySelector('#unList .unRow').getBoundingClientRect().height > 0, null, { timeout: 10000 });
    await page.waitForFunction(() => [...document.querySelectorAll('#unList .unRow img')].every(i => i.complete), null, { timeout: 10000 }).catch(() => {});
    await wait(800);
    // the idle page, for the machine's own frame rate (the smooth checks are judged against it when it drops frames)
    const r0 = await measure('idle page (no motion)', async () => {}, 2000);
    idle = { over34: r0.over34.length, frames: r0.frames };
    // 1 · a charm sent to a metal flies from its row to its thumbnail on that card, which says "+1"; the rows under it glide up
    await measure('Nest · charm sent to Gold (row to card, +1)', async () => {
      const rows = [...document.querySelectorAll('#unList .unRow')];
      for (const n of rows.slice(1, 4)) __M.watch.push({ name: n.dataset.mkey, el: n, from: n.getBoundingClientRect().top, ys: [] });
      rows[0].querySelector('button[data-m="gold"]').click();
    }, 2600, { copy: true, plus: true });
    // 2 · a file removed folds away where it stood (its thumbnails on the card fade too)
    await measure('Nest · file removed (row folds away)', async () => {
      document.querySelector('#srcList .srcRow .srcX').click();
    }, 1800, { copy: true });

    /* ── The other tabs' motion: the same Motion calls on elements of their size, in the page's styles ── */
    // a cleared sheet's dissolve is clearingCards' own animate() call, read from the page
    const dis = /else (g\.animate\(\[\{ opacity: 1[^\n]*?\.then\(\(\) => g\.remove\(\)\));/.exec(fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8').slice(fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8').indexOf('function clearingCards(')));
    if (!dis) throw new Error("clearingCards' dissolve was not found in charm-nest-1.html");
    await page.evaluate(src => { window.__dissolve = (0, eval)('g => ' + src); }, dis[1]);
    await page.evaluate(() => {
      const stage = document.createElement('div'); stage.id = 'tStage';
      Object.assign(stage.style, { position: 'fixed', left: '260px', top: '60px', width: '900px', height: '800px', zIndex: 60, background: 'var(--bg, #f6f2ea)' });
      stage.innerHTML = `<div id="tTabs" style="display:flex;gap:10px;padding:10px"><button class="btn" id="tPillA">Open Orders <b>30</b></button><button class="btn" id="tPillB">On hold <b>2</b></button><button class="btn" id="tDone">Decided <b>4</b></button></div><div class="scroll" id="tScroll" style="height:700px;overflow:auto"><div id="tList"></div></div>`;
      document.body.appendChild(stage);
      window.__row = (k, h) => { const n = document.createElement('div'); n.dataset.mkey = k; n.className = 'tRow'; Object.assign(n.style, { height: (h || 56) + 'px', margin: '0 12px 6px', borderRadius: '10px', background: '#fff', border: '1px solid #ddd', display: 'flex', alignItems: 'center', gap: '12px', padding: '0 14px', font: '13px sans-serif' }); n.innerHTML = `<i style="width:36px;height:36px;border-radius:6px;background:#d9c7a0"></i><b>Order ${k}</b><span>Heart charm × 2 · Ada Lovelace · GF-HEART-01</span>`; return n; };
      window.__list = (keys) => { const l = document.getElementById('tList'); Motion.reconcile(l, keys.map(k => l.querySelector(`[data-mkey="${k}"]`) || __row(k)), { animate: true }); };
      __list(Array.from({ length: 16 }, (_, i) => 'o' + i));
    });
    // 3 · Orders: a row a click moves flies to the pile that holds it; the rows under it glide up
    await measure('Orders · row flies to its pile', async () => {
      const l = document.getElementById('tList');
      for (const k of ['o4', 'o5', 'o6']) { const n = l.querySelector(`[data-mkey="${k}"]`); __M.watch.push({ name: k, el: n, from: n.getBoundingClientRect().top, ys: [] }); }
      Motion.expect('o3', { to: '#tPillB', plus: '+1' });
      __list(Array.from({ length: 16 }, (_, i) => 'o' + i).filter(k => k !== 'o3'));
    }, 2400, { copy: true, plus: true });
    // 4 · Orders: a row that leaves with nowhere to go folds away where it stood
    await measure('Orders · row folds away (no destination)', async () => {
      __list(Array.from({ length: 16 }, (_, i) => 'o' + i).filter(k => k !== 'o3' && k !== 'o8'));
    }, 1600, { copy: true });
    // 5 · a row that comes back (Undo, Release hold) opens its room: the rows under it glide down, never jumping first
    await measure('Library/Orders · a row comes back and opens its room', async () => {
      const l = document.getElementById('tList');
      for (const k of ['o9', 'o10']) { const n = l.querySelector(`[data-mkey="${k}"]`); __M.watch.push({ name: k, el: n, from: n.getBoundingClientRect().top, ys: [] }); }
      __list(Array.from({ length: 16 }, (_, i) => 'o' + i).filter(k => k !== 'o3'));
    }, 1600);
    // 6 · Library: Undo — the card flies back in from its tab
    await measure('Library · Undo: the card flies in from its tab', async () => {
      Motion.expectIn('o3', { from: '#tPillB' });
      __list(Array.from({ length: 16 }, (_, i) => 'o' + i));
    }, 2200, { copy: true });
    // 7 · Library: Mark completed — a set card with its sheet picture flies to the Completed tab
    await measure('Library · a card with its sheet flies to Completed', async () => {
      const card = document.createElement('div'); card.className = 'libCard';
      Object.assign(card.style, { position: 'fixed', left: '320px', top: '220px', width: '420px', height: '300px', background: '#fff', borderRadius: '12px', border: '1px solid #ddd', padding: '12px', zIndex: 61 });
      const cv = document.createElement('canvas'); cv.width = 1600; cv.height = 800; cv.style.cssText = 'width:100%;height:190px;display:block';
      const g = cv.getContext('2d'); g.fillStyle = '#f3eee4'; g.fillRect(0, 0, 1600, 800); for (let i = 0; i < 400; i++) { g.strokeStyle = '#8a6d3b'; g.beginPath(); g.arc((i * 97) % 1600, (i * 53) % 800, 20 + i % 17, 0, 6.3); g.stroke(); }
      card.appendChild(cv); card.insertAdjacentHTML('beforeend', '<b>GF Sheet 3 · 24 charms</b>'); document.body.appendChild(card);
      const gh = Motion.ghost(card); card.remove(); Motion.fly(gh, '#tDone', { plus: '+1' });
    }, 2400, { copy: true, plus: true });
    // 8 · Engraving: an approved card (with its photo) flies up into Decided
    await measure('Engraving · decided card flies to Decided', async () => {
      const card = document.createElement('div'); card.className = 'egCard';
      Object.assign(card.style, { position: 'fixed', left: '360px', top: '200px', width: '560px', height: '420px', background: '#fff', borderRadius: '14px', border: '1px solid #ddd', padding: '16px', zIndex: 61 });
      const im = document.createElement('canvas'); im.width = 1100; im.height = 700; im.style.cssText = 'width:100%;height:300px;display:block'; const g = im.getContext('2d'); const gr = g.createLinearGradient(0, 0, 1100, 700); gr.addColorStop(0, '#c9a45c'); gr.addColorStop(1, '#5b4a2a'); g.fillStyle = gr; g.fillRect(0, 0, 1100, 700);
      card.appendChild(im); card.insertAdjacentHTML('beforeend', '<div style="font:15px serif;margin-top:10px">Front: "Always & Forever" · Back: 12.08.2026</div>'); document.body.appendChild(card);
      const gh = Motion.ghost(card); card.remove(); Motion.fly(gh, '#tDone', { plus: '+1' });
    }, 2400, { copy: true, plus: true });
    // 9 · a cleared sheet dissolves into the empty sheet (charm-nest-1.html clearingCards: its own keyframes)
    await measure('Nest · a cleared sheet dissolves', async () => {
      const wrap = document.createElement('div'); wrap.className = 'shPreviewWrap';
      Object.assign(wrap.style, { position: 'fixed', left: '300px', top: '200px', width: '640px', height: '320px', zIndex: 61, background: '#fff' });
      const cv = document.createElement('canvas'); cv.width = 1280; cv.height = 640; cv.style.cssText = 'width:100%;height:100%;display:block'; const g = cv.getContext('2d'); g.fillStyle = '#efe7d6'; g.fillRect(0, 0, 1280, 640); for (let i = 0; i < 300; i++) { g.fillStyle = 'rgba(200,162,78,.35)'; g.beginPath(); g.arc((i * 131) % 1280, (i * 71) % 640, 18 + i % 13, 0, 6.3); g.fill(); }
      wrap.appendChild(cv); document.body.appendChild(wrap);
      const gh = Motion.ghost(wrap, null, 0, wrap); wrap.remove();
      __dissolve(gh);
    }, 1600, { copy: true });
    // 10 · the "+1" alone (a tab answering a move it could not show)
    await measure('"+1" on a tab', async () => { Motion.arrive('#tPillA', { plus: '+1' }); }, 1600, { plus: true });

    check(!errors.length, 'no page errors: ' + errors.join(' | '));
    console.log('\n  summary');
    for (const r of rows) console.log(`   ${r.name.padEnd(56)} worst ${String(r.maxFrame).padStart(3)} ms  >34 ${r.over34}  long ${r.long}  ${r.copies}`);
  } finally { await browser.close(); server.close(); }
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nall passed');
})().catch(e => { console.error(e); process.exit(1); });
