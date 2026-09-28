// Every pop-up in the sorter, measured in a real Chromium (Paul, 28 Sep: "some of the animations were not exactly
// visible and they were jerky"). Each window that grows out of what opened it and goes back into it
// (Motion.dialogOpen / dialogClose, charm-nest-motion.js) opens from its real button (or, for a window whose opener
// needs data this page has not got, from a button of the test's own through its real open function), then closes by
// its own button and by Esc, three times. Every frame of every flight is sampled, and each flight must be
//   · visible: the growing surface (or the closing copy) moves for at least 280 ms, its rect mid-way differing from its
//     first and last;
//   · smooth: no long task over 50 ms on any flight (the first close of a session included), and no frame over 34 ms on
//     its best round (this machine is shared: one slow frame on one round is noise, one on every round is not);
//   · cheap: only transform, opacity and clip-path are animated on the window, its surface and its copy;
//   · whole: a double click opens one window and one flight.
// Also: a window opened from something nearly its own size (a sheet's canvas) grows out of the point pressed, not out of
// that whole box (nothing was seen to grow); a menu's pop is on transform and opacity only (its origin in the keyframes
// kept it off the compositor).
//   node tests/charm-nest/adv-dialog-visible.cjs [playwright-core dir]
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json' };
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split('?')[0]);
  if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
  // (MOTION_FILE: another charm-nest-motion.js to measure, e.g. an older one)
  const f = u === '/charm-nest-motion.js' && process.env.MOTION_FILE ? process.env.MOTION_FILE : path.join(root, u === '/' ? 'charm-nest-1.html' : u);
  if (!fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
}).listen(0);

const OK_PROPS = new Set(['transform', 'opacity', 'clipPath', 'offset', 'easing', 'composite', 'computedOffset']);
const ROUNDS = ['button', 'Esc', 'button'];

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], table = [];
  const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => r.abort());
    await context.addInitScript(() => {
      window.__lt = [];
      try { new PerformanceObserver(l => { for (const e of l.getEntries()) window.__lt.push({ at: e.startTime, ms: e.duration }); }).observe({ type: 'longtask' }); } catch (_) {}
      // what flies: the window, its surface, its parts, its closing copy and shade, a menu's panel (not a button's own
      // hover transition)
      const ours = t => t && t.nodeType === 1 && (t.matches('dialog, .mdSurface, .mdSurface *, .mdGhost, .mdGhostBack, details[open] > :not(summary)') || !!t.closest('dialog[open]'));
      const tag = t => t.id ? '#' + t.id : (t.className && typeof t.className === 'string' ? '.' + t.className.split(' ')[0] : t.tagName.toLowerCase());
      window.__rec = {
        start(kind) {
          const R = this; R.t0 = performance.now(); R.frames = []; R.samples = []; R.props = new Set(); R.on = true; R.lt0 = window.__lt.length;
          const pick = () => kind === 'open' ? document.querySelector('.mdSurface') : [...document.querySelectorAll('.mdGhost')].pop();
          const loop = t => {
            if (!R.on) return;
            R.frames.push(t);
            const el = pick();
            if (el) { const r = el.getBoundingClientRect(); R.samples.push({ t: t - R.t0, x: Math.round(r.left), y: Math.round(r.top), w: Math.round(r.width), h: Math.round(r.height), o: +getComputedStyle(el).opacity }); }
            for (const a of document.getAnimations()) {
              try { if (window.CSSTransition && a instanceof CSSTransition) continue; const t = a.effect.target; if (!ours(t)) continue; for (const k of a.effect.getKeyframes()) for (const p of Object.keys(k)) R.props.add(p + '@' + tag(t)); } catch (_) {}
            }
            requestAnimationFrame(loop);
          };
          requestAnimationFrame(loop);
        },
        stop() {
          const R = this; R.on = false;
          const d = R.frames.slice(1).map((t, i) => t - R.frames[i]);
          const S = R.samples, key = s => s ? `${s.x},${s.y},${s.w},${s.h},${s.o.toFixed(2)}` : '';
          const moving = S.filter((s, i) => i && key(s) !== key(S[i - 1]));
          const span = moving.length ? moving[moving.length - 1].t - S[0].t : 0;
          // (while it runs: from its first frame; the click's own work before that holds nothing up mid-flight)
          const from = S.length ? R.t0 + S[0].t - 1 : R.t0, lt = window.__lt.slice(R.lt0).filter(e => e.at + e.ms > from), pre = window.__lt.slice(R.lt0).filter(e => e.at + e.ms <= from && e.at >= R.t0 - 5);
          return { frames: d.length, maxFrame: Math.round(Math.max(0, ...d)), longtasks: lt.map(e => Math.round(e.ms)), before: pre.map(e => Math.round(e.ms)), samples: S.length, span: Math.round(span), first: key(S[0]), mid: key(S[Math.floor(S.length / 2)]), last: key(S[S.length - 1]), props: [...R.props] };
        }
      };
    });
    const page = await context.newPage();
    const errors = []; page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.Motion && window.Motion.dialogOpen && window.RunHistory);
    await page.waitForTimeout(2500);   // the page settles (its own start-up work is not the pop-ups')
    const wait = ms => page.waitForTimeout(ms);
    await page.evaluate(() => {
      const b = Object.assign(document.createElement('button'), { textContent: 'Open it', className: 'btn gold', id: 'tOpener' });
      Object.assign(b.style, { position: 'fixed', left: '60px', top: '140px', zIndex: 5 }); document.body.appendChild(b);
    });
    const viaOpener = fn => async () => { await page.hover('#tOpener'); await page.mouse.down(); await page.mouse.up(); await page.evaluate(fn); };
    const flight = async (kind, act, ms) => { await page.evaluate(k => __rec.start(k), kind); await act(); await wait(ms); return page.evaluate(() => __rec.stop()); };

    const windows = [
      { label: 'Settings', sel: '#dlgSettings', open: async () => { await page.click('#moreMenu > summary'); await wait(400); await page.click('#btnSettings'); }, close: () => page.click('#dlgSettings button[value=cancel]') },
      { label: 'Sets window', sel: '#histDlg', open: async () => { if (await page.isVisible('#btnEarlierSets')) await page.click('#btnEarlierSets'); else await viaOpener(() => RunHistory.show(''))(); }, close: () => page.click('#histDlg [data-close]') },
      { label: 'Report', sel: '#dlgReport', open: viaOpener(() => { document.getElementById('rpTitle').textContent = 'Gold · Sheet 1 · report'; openDlg(document.getElementById('dlgReport')); }), close: () => page.click('#rpClose') },
      { label: 'Split order question', sel: 'dialog.splitDlg', open: viaOpener(() => { const d = document.createElement('dialog'); d.className = 'splitDlg'; d.innerHTML = '<div class="dlg"><div class="dlgHead"><h3 id="splitT">An order is on two sheets</h3></div><div class="dlgBody"><p>Gold Sheet 1 shares an order with a sheet that is not in the current set.</p></div><div class="dlgFoot"><div class="left"><button type="button" class="btn ghost sm" data-k="cancel">Cancel</button></div><button type="button" class="btn ghost sm" data-k="one">Only Gold Sheet 1</button><button type="button" class="btn sage" data-k="all">Include both</button></div></div>'; document.body.appendChild(d); const done = () => { if (d.open) d.close(); d.remove(); }; d.querySelectorAll('[data-k]').forEach(b => b.onclick = done); d.addEventListener('cancel', e => { e.preventDefault(); done(); }); d.showModal(); d.querySelector('[data-k=all]').focus(); }), close: () => page.click('dialog.splitDlg [data-k=cancel]') },
      { label: 'Set preview', sel: '#setPreview', open: viaOpener(() => SetPicker.preview({ name: 'Set 4', day: '2026-09-28', sheets: [], status: 'manual' })), close: () => page.click('#setPreview form.x button') },
      { label: 'Customer conversation', sel: 'dialog.cmDlg', open: viaOpener(() => CustomerMail.openConversation({ receiptId: '4176200172', scope: 'order' }, null)), close: () => page.click('dialog.cmDlg [data-x]') }
    ];
    for (const w of windows) {
      const res = { open: [], close: [] };
      for (const how of ROUNDS) {
        res.open.push(await flight('open', w.open, 1150));
        const open = await page.evaluate(s => { const d = document.querySelector(s); return !!(d && d.open); }, w.sel);
        if (!open) { check(false, `${w.label}: opened`); break; }
        res.close.push(await flight('close', how === 'Esc' ? () => page.keyboard.press('Escape') : w.close, 900));
        const shut = await page.evaluate(s => { const d = document.querySelector(s); return !(d && d.open); }, w.sel);
        if (!shut) { check(false, `${w.label}: closed with ${how}`); break; }
        await wait(500);
      }
      for (const kind of ['open', 'close']) {
        const L = res[kind]; if (!L.length) continue;
        const loaded = os.loadavg()[0] > os.cpus().length;
        const tag = `${w.label} ${kind}`, vis = L.every(r => r.samples >= 10 && r.span >= 280 && r.mid !== r.first && r.mid !== r.last);
        const lts = L.flatMap(r => r.longtasks), best = Math.min(...L.map(r => r.maxFrame)), bad = [...new Set(L.flatMap(r => r.props))].filter(p => !OK_PROPS.has(p.split('@')[0]));
        check(vis, `${tag}: visible every time (moves ${L.map(r => r.span).join('/')} ms; first ${L[0].first} · mid ${L[0].mid} · last ${L[0].last})`);
        const pre = L.flatMap(r => r.before);
        check(!lts.some(x => x > 50), `${tag}: no long task while it flies (${lts.join(',') || 'none'}; the first ${kind} of the session included${pre.length ? '; before its first frame: ' + pre.join(',') : ''})`);
        // (frames are judged on a machine that is not busy with other work; a loaded one is reported, not judged)
        if (loaded) console.log(`  --   ${tag}: frames not judged, this machine is loaded (load ${os.loadavg()[0].toFixed(1)} on ${os.cpus().length} CPUs): worst frame per round ${L.map(r => r.maxFrame).join('/')} ms`);
        else check(best <= 34, `${tag}: smooth (worst frame per round ${L.map(r => r.maxFrame).join('/')} ms)`);
        check(!bad.length, `${tag}: only transform / opacity / clip-path (${bad.join(', ') || 'yes'})`);
        table.push([w.label, kind, L.map(r => r.span).join('/'), L.map(r => r.maxFrame).join('/'), lts.join(',') || '-', pre.join(',') || '-', bad.join(' ') || '-']);
      }
    }

    // a window opened from a sheet's canvas grows out of the point pressed, and goes back to it
    await page.evaluate(() => {
      const c = document.createElement('canvas'); c.width = 900; c.height = 500; c.id = 'tCanvas'; Object.assign(c.style, { position: 'fixed', left: '200px', top: '150px', width: '900px', height: '500px', zIndex: 5 }); document.body.appendChild(c);
      c.onclick = () => { document.getElementById('rpTitle').textContent = 'Charm'; openDlg(document.getElementById('dlgReport')); };
    });
    await page.mouse.move(420, 260);
    const cOpen = await flight('open', async () => { await page.mouse.down(); await page.mouse.up(); }, 1150);
    const cClose = await flight('close', () => page.click('#rpClose'), 900);
    const first = cOpen.first.split(',').map(Number), last = cClose.last.split(',').map(Number);
    check(first[2] <= 80 && Math.abs(first[0] + first[2] / 2 - 420) < 30 && Math.abs(first[1] + first[3] / 2 - 260) < 30, `opened from a big canvas: grows out of the point pressed (first ${cOpen.first}, not the whole canvas)`);
    check(last[2] <= 120 && Math.abs(last[0] + last[2] / 2 - 420) < 60, `closed: goes back to that point (last ${cClose.last})`);
    await page.evaluate(() => document.getElementById('tCanvas').remove());
    await wait(500);

    // a menu's pop (the Workspace menu): transform and opacity only
    const menu = await page.evaluate(async () => {
      const m = document.getElementById('moreMenu'); m.open = true; await new Promise(r => setTimeout(r, 30));
      const list = m.querySelector('.moreList'), props = new Set(); for (const a of list.getAnimations()) for (const k of a.effect.getKeyframes()) for (const p of Object.keys(k)) props.add(p);
      await new Promise(r => setTimeout(r, 400)); m.open = false; return { props: [...props], origin: list.style.transformOrigin };
    });
    check(menu.props.includes('transform') && menu.props.every(p => OK_PROPS.has(p)), `the Workspace menu pops on transform and opacity only (${menu.props.join(', ')})`);
    check(menu.origin === '', `the menu panel is given back its own transform-origin ("${menu.origin}")`);

    // never a pop-up over a pop-up, never a flight over a flight: a second open while the first still grows
    const dbl = await page.evaluate(async () => {
      document.getElementById('btnSettings').click(); await new Promise(r => setTimeout(r, 60)); document.getElementById('btnSettings').click();
      await new Promise(r => setTimeout(r, 30));
      const out = { surfaces: document.querySelectorAll('.mdSurface').length, open: document.querySelectorAll('dialog[open]').length };
      await new Promise(r => setTimeout(r, 1200)); out.left = document.querySelectorAll('.mdSurface').length; document.getElementById('dlgSettings').close();
      await new Promise(r => setTimeout(r, 900)); out.ghosts = document.querySelectorAll('.mdGhost').length; return out;
    });
    check(dbl.surfaces === 1 && dbl.open === 1 && dbl.left === 0 && dbl.ghosts === 0, `a double click on Settings opens one window, one flight (${JSON.stringify(dbl)})`);
    check(!errors.length, `no page errors (${errors.join(' | ')})`);
  } finally { await browser.close(); server.close(); }
  console.log('\nwindow | flight | moves (ms) per round | worst frame (ms) per round | long tasks in flight (ms) | before its first frame (ms) | other properties');
  for (const r of table) console.log(r.join(' | '));
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nall passed');
})().catch(e => { console.error(e); process.exit(1); });
