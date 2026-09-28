// Adversarial checks of the app-wide pop-up motion (Motion.dialogOpen / dialogClose and the showModal / close wrapper,
// charm-nest-motion.js) on real <dialog>s in the sorter page: the window opens and closes at once whatever the motion
// does; returnValue, form method="dialog", Esc (and a cancel that is refused) and focus behave as the browser's own; a
// window closed while it is still growing leaves an opaque copy (not its bare contents); Esc on a window whose cancel
// handler closes it itself leaves one copy, not two (askSplit, the custom designs window, the Sets window); a window
// closed just after another has opened on top of it never draws its copy over the new one; a window taken out of the page
// mid-grow, rapid open/close/open and reduced motion leave nothing behind; the sheet window, the order window and the
// photo viewer are left to their own motion.
//   node tests/charm-nest/adv-dialog-motion.cjs [playwright-core dir]
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json' };
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split('?')[0]);
  if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
  const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
}).listen(0);

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 860 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => r.abort());
    // every closing copy's (closed) shadow root, kept for the checks
    await context.addInitScript(() => {
      const a = Element.prototype.attachShadow; window.__ghosts = [];
      Element.prototype.attachShadow = function (o) { const r = a.call(this, o); if (this.classList && this.classList.contains('mdGhost')) window.__ghosts.push({ host: this, root: r, at: performance.now() }); return r; };
    });
    const page = await context.newPage();
    const errors = []; page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.Motion && window.Motion.dialogOpen);
    await page.evaluate(() => {
      window.mk = (cls, html) => { const d = document.createElement('dialog'); if (cls) d.className = cls; d.innerHTML = html || '<div class="dlg"><div class="dlgHead"><h3>Test</h3></div><div class="dlgBody"><p>Body</p><input id="tIn"></div><div class="dlgFoot"><button type="button" class="btn" data-x>Close</button></div></div>'; document.body.appendChild(d); return d; };
      window.opener = Object.assign(document.createElement('button'), { textContent: 'Open it', className: 'btn gold' });
      Object.assign(window.opener.style, { position: 'fixed', left: '40px', top: '120px', zIndex: 5 });
      document.body.appendChild(window.opener);
    });
    const wait = ms => page.waitForTimeout(ms);

    // 1 · closed while it still grows: the copy that goes back has the window's own colour, not a see-through box
    const r1 = await page.evaluate(async () => {
      const d = mk(); opener.focus(); Motion.from(d, opener); d.showModal();
      await new Promise(r => setTimeout(r, 120));
      const growing = d.classList.contains('mdGrow'), n = __ghosts.length; d.close();
      const g = __ghosts[n], box = g && g.root.firstElementChild;
      const out = { growing, open: d.open, ghost: !!g, bg: box ? box.style.backgroundColor : '', shadow: box ? box.style.boxShadow : '', want: getComputedStyle(d).backgroundColor, focus: document.activeElement === opener };
      d.remove(); return out;
    });
    check(r1.growing && !r1.open && r1.ghost, `closed mid-grow: closes at once and leaves a copy (${JSON.stringify({ growing: r1.growing, open: r1.open, ghost: r1.ghost })})`);
    check(r1.bg && !/rgba\(0, 0, 0, 0\)|transparent/.test(r1.bg) && r1.bg === r1.want, `closed mid-grow: the copy is opaque in the window's colour (copy ${r1.bg || 'none'}, window ${r1.want})`);
    check(r1.shadow && r1.shadow !== 'none', `closed mid-grow: the copy keeps the window's shadow (${r1.shadow || 'none'})`);
    check(r1.focus, 'closed mid-grow: focus goes back to what opened it');
    await wait(700);

    // 2 · Esc on a window whose cancel handler refuses the browser's close and closes (and removes) it itself: one copy
    await page.evaluate(() => { window.__d2 = mk('splitDlg'); __d2.addEventListener('cancel', e => { e.preventDefault(); if (__d2.open) __d2.close(); __d2.remove(); }); opener.focus(); __d2.showModal(); window.__n2 = __ghosts.length; });
    await wait(900);
    await page.keyboard.press('Escape');
    await wait(250);
    const r2 = await page.evaluate(() => ({ open: __d2.open, copies: __ghosts.length - __n2, shades: document.querySelectorAll('#motionLayer > .mdGhostBack').length }));
    check(!r2.open && r2.copies === 1, `Esc + own close + removal: one copy goes back, not ${r2.copies}`);
    check(r2.shades <= 1, `Esc + own close + removal: one shade lifts, not ${r2.shades}`);
    await wait(700);

    // 2b · the same, the window staying in the page (the custom designs window, the Sets window)
    await page.evaluate(() => { window.__d3 = mk(); __d3.addEventListener('cancel', e => { e.preventDefault(); __d3.close(); }); opener.focus(); __d3.showModal(); window.__n3 = __ghosts.length; });
    await wait(900);
    await page.keyboard.press('Escape');
    await wait(250);
    const r3 = await page.evaluate(() => ({ open: __d3.open, copies: __ghosts.length - __n3, focus: document.activeElement === opener }));
    check(!r3.open && r3.copies === 1, `Esc + own close: one copy, not ${r3.copies}`);
    check(r3.focus, 'Esc + own close: focus goes back to what opened it');
    await wait(700);

    // 3 · returnValue: close(v), a form's method="dialog" button, Esc; a refused cancel keeps the window open
    const r4 = await page.evaluate(async () => {
      const d = mk('', '<form method="dialog" class="dlg"><div class="dlgHead"><h3>Form</h3></div><div class="dlgBody"><input name="a"></div><div class="dlgFoot"><button value="cancel" formnovalidate>Close</button><button value="save" id="fSave">Save</button></div></form>');
      const out = {}; let closes = 0; d.addEventListener('close', () => closes++);
      d.showModal(); d.close('picked'); out.rv1 = d.returnValue; out.open1 = d.open;
      await new Promise(r => setTimeout(r, 50));
      d.returnValue = ''; d.showModal(); d.querySelector('#fSave').click(); out.rv2 = d.returnValue; out.open2 = d.open;
      await new Promise(r => setTimeout(r, 50));
      out.closes = closes; d.remove(); return out;
    });
    check(r4.rv1 === 'picked' && !r4.open1, `close("picked"): returnValue ${r4.rv1}, closed at once`);
    check(r4.rv2 === 'save' && !r4.open2, `method="dialog" Save: returnValue ${r4.rv2}, closed at once`);
    check(r4.closes === 2, `one close event per closing (${r4.closes})`);
    await wait(600);
    await page.evaluate(() => { window.__d5 = mk(); __d5.returnValue = 'kept'; __d5.addEventListener('cancel', e => e.preventDefault(), { once: true }); window.__closes5 = 0; __d5.addEventListener('close', () => __closes5++); opener.focus(); __d5.showModal(); });
    await wait(200);
    await page.keyboard.press('Escape');
    await wait(300);
    const r5a = await page.evaluate(() => ({ open: __d5.open, surf: !!__d5.querySelector('.mdSurface'), grow: __d5.classList.contains('mdGrow') }));
    check(r5a.open, 'a refused Esc keeps the window open');
    await wait(900);
    const r5c = await page.evaluate(() => ({ grow: __d5.classList.contains('mdGrow'), surf: !!__d5.querySelector('.mdSurface'), bg: getComputedStyle(__d5).backgroundColor }));
    check(!r5c.grow && !r5c.surf && !/rgba\(0, 0, 0, 0\)/.test(r5c.bg), `after a refused Esc the window settles opaque (${JSON.stringify(r5c)})`);
    await page.keyboard.press('Escape');
    await wait(250);
    const r5b = await page.evaluate(() => ({ open: __d5.open, rv: __d5.returnValue, closes: __closes5, focus: document.activeElement === opener }));
    // (Chromium's own Esc leaves returnValue "", with or without the wrapper)
    check(!r5b.open && r5b.rv === '' && r5b.closes === 1, `Esc closes it, returnValue as the browser leaves it (${JSON.stringify(r5b)})`);
    check(r5b.focus, 'Esc: focus goes back to what opened it');
    await page.evaluate(() => __d5.remove());
    await wait(600);

    // 4 · a window opened on top of another, then the one under it closed: its copy is not drawn over the new window
    const r6 = await page.evaluate(async () => {
      const a = mk(), b = mk('', '<div class="dlg"><div class="dlgHead"><h3>B</h3></div><div class="dlgBody">b</div></div>');
      a.showModal(); await new Promise(r => setTimeout(r, 700));
      const n = __ghosts.length; b.showModal(); a.close();
      const g = __ghosts[n], inB = !!(g && b.contains(g.host)), shadeInB = !!b.querySelector('.mdGhostBack');
      await new Promise(r => setTimeout(r, 700)); b.close(); a.remove(); await new Promise(r => setTimeout(r, 600)); b.remove();
      return { ghost: !!g, inB, shadeInB };
    });
    check(r6.ghost && !r6.inB && !r6.shadeInB, `a window closed under a newer one stays under it (copy in the new window: ${r6.inB}, shade: ${r6.shadeInB})`);

    // 4b · a window opened from inside another goes back into its button there, over the window under it (as it was)
    const r7 = await page.evaluate(async () => {
      const a = mk('', '<div class="dlg"><div class="dlgHead"><h3>A</h3></div><div class="dlgBody"><button type="button" id="inA">More</button></div></div>'), b = mk();
      a.showModal(); await new Promise(r => setTimeout(r, 700));
      const btn = a.querySelector('#inA'); btn.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true })); b.showModal();
      await new Promise(r => setTimeout(r, 700)); const n = __ghosts.length; b.close();
      const g = __ghosts[n], inA = !!(g && a.contains(g.host)), openA = a.open, openB = b.open;
      await new Promise(r => setTimeout(r, 600)); a.close(); await new Promise(r => setTimeout(r, 600)); a.remove(); b.remove();
      return { inA, openA, openB };
    });
    check(r7.inA && r7.openA && !r7.openB, `inner window closes back into the outer one (${JSON.stringify(r7)})`);

    // 5 · taken out of the page mid-grow; rapid open / close / open: nothing left behind, and the window ends opaque
    const r8 = await page.evaluate(async () => {
      const d = mk(); d.showModal(); await new Promise(r => setTimeout(r, 60)); d.remove();
      await new Promise(r => setTimeout(r, 50));
      const e = mk(); for (let i = 0; i < 4; i++) { e.showModal(); await new Promise(r => setTimeout(r, 30)); e.close(); } e.showModal();
      const surfaces = e.querySelectorAll('.mdSurface').length;
      await new Promise(r => setTimeout(r, 1300));
      const out = { open: e.open, surfaces, after: e.querySelectorAll('.mdSurface').length, grow: e.classList.contains('mdGrow'), bg: getComputedStyle(e).backgroundColor, left: document.querySelectorAll('.mdSurface').length };
      e.close(); await new Promise(r => setTimeout(r, 700)); e.remove();
      out.ghostsLeft = document.querySelectorAll('.mdGhost,.mdGhostBack').length; return out;
    });
    check(r8.open && r8.surfaces === 1 && r8.after === 0 && !r8.grow && r8.left === 0 && r8.ghostsLeft === 0, `removed mid-grow and rapid open/close/open leave nothing (${JSON.stringify(r8)})`);

    // 5b · the real windows: Settings (Save, its Close button, Esc) and the Sets window (Esc), each closing at once, one copy
    const settings = async how => {
      await page.evaluate(() => { document.getElementById('btnSettings').click(); window.__ns = __ghosts.length; });
      await wait(250);
      const open = await page.evaluate(() => document.getElementById('dlgSettings').open);
      if (how === 'esc') await page.keyboard.press('Escape'); else await page.click(how === 'save' ? '#btnSaveSettings' : '#dlgSettings button[value=cancel]');
      await wait(250);
      return page.evaluate(o => { const d = document.getElementById('dlgSettings'); return { opened: o, open: d.open, rv: d.returnValue, copies: __ghosts.length - __ns, rail: document.documentElement.dataset.rail || '' }; }, open);
    };
    for (const how of ['save', 'close', 'esc']) {
      const s = await settings(how);
      check(s.opened && !s.open && s.copies === 1, `Settings ${how}: opened, closed at once, one copy (${JSON.stringify(s)})`);
      if (how === 'close') check(s.rv === 'cancel', `Settings Close keeps its returnValue (${s.rv})`);
      await wait(600);
    }
    await page.evaluate(() => { RunHistory.show(); window.__nh = __ghosts.length; });
    await wait(300);
    await page.keyboard.press('Escape');
    await wait(250);
    const sw = await page.evaluate(() => { const d = document.getElementById('histDlg'); return { open: d && d.open, copies: __ghosts.length - __nh }; });
    check(sw.open === false && sw.copies === 1, `Sets window Esc: closed at once, one copy (${JSON.stringify(sw)})`);
    await wait(600);

    // 6 · left to their own motion: the sheet window, the order window, the photo viewer (data-no-grow)
    const r9 = await page.evaluate(async () => {
      const out = {};
      for (const [k, d] of [['sheetWin', mk('sheetWin')], ['noGrow', (() => { const x = mk(); x.setAttribute('data-no-grow', ''); return x; })()], ['orderWin', document.getElementById('orderWin')]]) {
        const n = __ghosts.length; d.showModal(); const a = { mdIn: d.classList.contains('mdIn'), surf: !!d.querySelector('.mdSurface') }; d.close(); a.ghost = __ghosts.length > n; a.open = d.open; out[k] = a;
        if (k !== 'orderWin') d.remove();
      }
      return out;
    });
    check(Object.values(r9).every(a => !a.mdIn && !a.surf && !a.ghost && !a.open), `sheet window, order window and data-no-grow are left alone (${JSON.stringify(r9)})`);

    // 7 · reduced motion: a quick fade in, a fade out, the window open and closed at once
    await page.emulateMedia({ reducedMotion: 'reduce' });
    const r10 = await page.evaluate(async () => {
      const d = mk(); d.showModal(); const out = { open: d.open, grow: d.classList.contains('mdGrow'), surf: !!d.querySelector('.mdSurface') };
      await new Promise(r => setTimeout(r, 300)); d.close(); out.closed = !d.open; await new Promise(r => setTimeout(r, 400)); d.remove(); out.left = document.querySelectorAll('.mdGhost').length; return out;
    });
    check(r10.open && !r10.grow && !r10.surf && r10.closed && r10.left === 0, `reduced motion (${JSON.stringify(r10)})`);
    await page.emulateMedia({ reducedMotion: null });

    check(!errors.length, `no page errors (${errors.join(' | ')})`);
    await page.close();
  } finally { await browser.close(); server.close(); }
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nall passed');
})().catch(e => { console.error(e); process.exit(1); });
