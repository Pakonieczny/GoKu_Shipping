// LIVE FEEDBACK AND CONTROL IN THE CHARM EDITOR (charm-nest-charm-editor.js; Paul, 28 Oct 2026: "when I'm adding a circle or a square or any other primitive and then resizing it,
// I'm not seeing a real time read out of the size in the work area of the actually selected object and in the X and Y in the menu ... the ability to resize every object from its
// center-point ... by holding the control key while dragging"). One real Chromium page with nothing but the editor and the calendar charm (fixtures/charm-editor-harness.html);
// no server, no network. Run through the shared slot queue:
//   $SP/int/slot.sh node tests/charm-nest/ce-feedback.cjs
// 0 the extension points the Design & Engrave files plug into: a tool on the rail (its own and in the Shapes flyout), its gesture one undo step; a command in the Object
//   menu, the right-click menu, the bar and its key; an importer reached by the Import button, a dropped file and a pasted one; a panel beside the Layers; onOpen / onClose
// 1 drawing a rectangle on the calendar charm: the size chip beside it and the X / Y / W / H fields follow every frame (they used to stay blank); a line says its length
//   and angle, a polygon its sides (↑ ↓ while dragging); Ctrl-drag of a handle resizes about the centre, Alt too, Shift keeps the ratio, Ctrl + Shift both, and Ctrl pressed
//   mid-drag counts at once; the turn and the move read out too; the cursors turn with the part; the arrow keys' nudge is shown; hover and the live marquee
// 2 smart guides on a plain charm: a part snaps to another part's edge with the guide and the gap measured on it, to even spacing between two others (both gaps shown),
//   the Snap switch (and ⌘U) turns it off and on
// 3 frame work: the main-thread time of every pointer frame (handler + style and layout, and every animation-frame callback) while hovering, drawing, moving, resizing,
//   turning and sweeping a marquee, on the calendar charm and on a 300-path charm (everything moved at once too); each under 16 ms
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const HARNESS = 'file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html');
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 12).join('\n         ')); };
async function step(name, fn) { try { await fn(); pass(name); } catch (e) { fail(name, e); } }
const near = (a, b, eps) => Math.abs(a - b) <= eps;
const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const ptClient = (page, x, y) => page.evaluate(([x, y]) => window.CharmEditor.current.clientOf(x, y), [x, y]);
const mmPt = (page, xmm, ymm) => page.evaluate(([x, y]) => [window.fixture.X0 + x * window.fixture.PT, window.fixture.Y0 + y * window.fixture.PT], [xmm, ymm]);
const mmClient = async (page, xmm, ymm) => { const p = await mmPt(page, xmm, ymm); return ptClient(page, p[0], p[1]); };

/* frame work, measured in the page: a pointer frame's handler with the style and layout it leaves (window capture -> document bubble, which runs after the editor's own
   handler on the stage), and every animation-frame callback on its own */
function instrument() {
  window.__fw = []; window.__rw = [];
  window.addEventListener('pointermove', () => { window.__t0 = performance.now(); }, true);
  document.addEventListener('pointermove', () => { const r = document.querySelector('.ce-ui'); if (r) r.getBoundingClientRect(); window.__fw.push(performance.now() - window.__t0); });
  const raf = window.requestAnimationFrame.bind(window);
  window.requestAnimationFrame = cb => raf(t => { const a = performance.now(); try { cb(t); } finally { window.__rw.push(performance.now() - a); } });
}
const takeWork = page => page.evaluate(() => { const f = window.__fw.splice(0), r = window.__rw.splice(0); return { f, r }; });
const stats = a => { const s = a.slice().sort((x, y) => x - y); return { n: s.length, med: s.length ? s[s.length >> 1] : 0, p95: s.length ? s[Math.min(s.length - 1, Math.floor(s.length * 0.95))] : 0, max: s.length ? s[s.length - 1] : 0 }; };
const perf = [];
function record(label, w) {
  const f = stats(w.f), r = stats(w.r);
  perf.push({ label, f, r });
  console.log(`         ${label.padEnd(34)} pointer frames ${String(f.n).padStart(3)}: median ${f.med.toFixed(2)} ms, p95 ${f.p95.toFixed(2)}, max ${f.max.toFixed(2)} · animation frames: p95 ${r.p95.toFixed(2)}, max ${r.max.toFixed(2)}`);
  return { f, r };
}
/* frame work must stay inside one frame; the median and the 95th percentile are held to it (a loaded machine stalls a single frame now and then: the max is printed) */
const withinFrame = (m, label) => { assert.ok(m.f.n > 5, label + ': frames measured'); assert.ok(m.f.p95 < 16 && m.r.p95 < 16, `${label}: p95 ${m.f.p95.toFixed(2)} / ${m.r.p95.toFixed(2)} ms`); assert.ok(m.f.med < 8, `${label}: median ${m.f.med.toFixed(2)} ms`); };
const chip = page => page.evaluate(() => { const t = document.querySelector('.ce-ui .u-chip text'); return t ? t.textContent : null; });
const fields = page => page.evaluate(() => Object.fromEntries(['x', 'y', 'w', 'h', 'r'].map(k => { const el = document.querySelector(`.ce-root [data-f="${k}"]`); return [k, el.disabled ? null : el.value]; })));
const handleAt = (page, h) => page.evaluate(h => { const el = document.querySelector(`.ce-ui [data-h="${h}"]`); if (!el) return null; const r = el.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; }, h);
const boxMm = async (page, id) => page.evaluate(id => { const b = CharmEditor.current.box(id), f = CharmEditor.current.frame(), MM = 25.4 / 72; return [(b[0] - f.x0) * MM, (b[1] - f.y0) * MM, (b[2] - f.x0) * MM, (b[3] - f.y0) * MM]; }, id);
const lastId = page => page.evaluate(() => CharmEditor.current.items().slice(-1)[0].id);
const mmPx = page => page.evaluate(() => { const a = CharmEditor.current.clientOf(0, 0), b = CharmEditor.current.clientOf(72 / 25.4, 0); return b.x - a.x; });
const snapOn = page => page.evaluate(() => document.querySelector('.ce-snapb').classList.contains('is-on'));

/** A plain charm in mm (y down): a 40 mm square outline and the given rectangles, for snapping with nothing else in the way. */
function plainFace(rects) {
  const PT = 72 / 25.4, R = (x0, y0, x1, y1) => [[['m', [x0 * PT, y0 * PT]], ['l', [x1 * PT, y0 * PT]], ['l', [x1 * PT, y1 * PT]], ['l', [x0 * PT, y1 * PT]], ['h']]];
  return { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 40 * PT, hPt: 40 * PT },
    items: [{ id: 'outline', kind: 'path', role: 'cut', stroke: true, subpaths: R(1, 1, 39, 39) }].concat(rects.map(r => ({ id: r[0], kind: 'path', role: 'line', stroke: true, name: r[0], subpaths: R(r[1], r[2], r[3], r[4]) }))) };
}
/** 300 small curved parts in a 20 × 15 grid (four cubics each) on a 40 mm charm: a busy master. */
function busyFace() {
  const PT = 72 / 25.4, K = 0.5522847498, items = [{ id: 'outline', kind: 'path', role: 'cut', stroke: true, subpaths: [[['m', [PT, PT]], ['l', [39 * PT, PT]], ['l', [39 * PT, 39 * PT]], ['l', [PT, 39 * PT]], ['h']]] }];
  for (let i = 0; i < 300; i++) {
    const cx = (2.4 + (i % 20) * 1.8) * PT, cy = (3 + Math.floor(i / 20) * 2.3) * PT, rx = 0.62 * PT, ry = 0.8 * PT, kx = rx * K * (1 + (i % 5) * 0.1), ky = ry * K;
    items.push({ id: 'p' + i, kind: 'path', role: ['line', 'hatch', 'art'][i % 3], stroke: i % 3 === 0, fill: i % 3 !== 0, name: 'Part ' + i,
      subpaths: [[['m', [cx + rx, cy]], ['c', [cx + rx, cy + ky], [cx + kx, cy + ry], [cx, cy + ry]], ['c', [cx - kx, cy + ry], [cx - rx, cy + ky], [cx - rx, cy]],
        ['c', [cx - rx, cy - ky], [cx - kx, cy - ry], [cx, cy - ry]], ['c', [cx + kx, cy - ry], [cx + rx, cy - ky], [cx + rx, cy]], ['h']]] });
  }
  return { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 40 * PT, hPt: 40 * PT }, items };
}

/* the stand-in extensions: what a file such as charm-nest-ce-paths.js registers */
function registerExtensions() {
  const C = window.CharmEditor, log = window.__xlog = [];
  const rectOf = (a, b) => [[['m', [a.x, a.y]], ['l', [b.x, a.y]], ['l', [b.x, b.y]], ['l', [a.x, b.y]], ['h']]];
  C.tools.push({
    id: 'xbox', title: 'Box maker', key: 'b', cursor: 'cell', tip: 'Drags a box', hint: 'Drag a box (test tool)',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><rect x="5" y="5" width="14" height="14"/></svg>',
    onDown(ctx, ev, p) { this.a = p; this.box = ctx.addItems({ kind: 'path', role: 'line', name: 'X box', stroke: true, subpaths: rectOf(p, p) })[0]; log.push('down'); },
    onMove(ctx, ev, p) { if (!this.a || !ev.buttons) { log.push('hover'); return; } ctx.replaceItems({ [this.box]: Object.assign({}, ctx.entry(this.box).it, { subpaths: rectOf(this.a, p) }) }); log.push('move'); },
    onUp(ctx, ev, p) { this.a = null; log.push('up'); },
    onKey(ctx, ev) { if (ev.key === 'q') { log.push('key-q'); return true; } return false; },
    overlay(ctx) { return '<circle class="x-overlay" cx="10" cy="10" r="3"/>'; }
  });
  C.tools.push({ id: 'xstar', title: 'Star', flyout: 'shapes', icon: '<svg viewBox="0 0 24 24"><path d="M12 3 15 10 22 10 16 14 18 21 12 17 6 21 8 14 2 10 9 10Z"/></svg>', onDown() {}, onMove() {}, onUp() {} });
  C.tools.push({ id: 'xpen', title: 'Pen', flyout: 'Pen', key: 'p', onDown() {}, onMove() {}, onUp() {} });
  C.tools.push({ id: 'xpen2', title: 'Add anchor', flyout: 'Pen', onDown() {}, onMove() {}, onUp() {} });
  C.commands.xnudge = { title: 'Move right 1 mm', group: 'Test', key: 'Mod+Shift+K', bar: true, icon: '<svg viewBox="0 0 24 24"><path d="M4 12h16"/></svg>',
    enabled: ctx => ctx.selection().length > 0,
    run(ctx) { const m = ctx.model; const map = {}; for (const id of ctx.selection()) { const e = ctx.entry(id); map[id] = m.pushDown(e.it, m.tr(72 / 25.4, 0), ctx.geo); } ctx.replaceItems(map); log.push('cmd'); } };
  C.commands.xnever = { title: 'Never on', group: 'Test', enabled: () => false, run() { log.push('never'); } };
  C.importers.push({ id: 'xpng', title: 'Test image', accept: ['.png', 'image/'], run(ctx, file, info) { log.push('import:' + file.name + ':' + (info.via) + ':' + (info.at ? 'at' : '-'));
    return new Promise(r => setTimeout(r, 20)).then(() => { ctx.addItems({ kind: 'path', role: 'art', name: 'Imported ' + file.name, fill: true, subpaths: rectOf({ x: 110, y: 60 }, { x: 120, y: 70 }) }); }); } });
  C.panels.push({ id: 'xlib', title: 'Library', side: 'right', mount(ctx, el) { log.push('mount'); el.innerHTML = '<div class="x-panel-body">Designs</div>'; }, update(ctx) { log.push('update:' + ctx.selection().length); }, unmount() { log.push('unmount'); } });
  C.onOpen.push(ctx => log.push('open:' + (ctx === window.CharmEditor.current.ctx) + ':' + ctx.doc.items.length));
  C.onClose.push((ctx, r) => log.push('close:' + (r ? 'saved' : 'cancelled')));
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const newPage = async (w, h) => {
    const ctx = await browser.newContext({ viewport: { width: w, height: h }, reducedMotion: 'reduce' });
    await ctx.route(u => !u.href.startsWith('file://'), r => r.abort());
    const page = await ctx.newPage();
    page.__errs = [];
    page.on('pageerror', e => page.__errs.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') page.__errs.push('console: ' + m.text()); });
    await page.goto(HARNESS);
    return page;
  };
  const openIt = async page => { await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page); };
  const log = page => page.evaluate(() => window.__xlog.slice());

  /* ════ 0 · the extension points ════ */
  {
    const page = await newPage(1280, 720);
    await page.evaluate(registerExtensions);
    await openIt(page);
    await step('0.1 onOpen gets the ctx (the same as CharmEditor.current.ctx) with the face as a FaceDoc', async () => {
      assert.deepEqual((await log(page)).filter(s => s.startsWith('open')), ['open:true:45']);
    });
    await step('0.2 the tool: a rail button before More, armed by its key, its cursor and hint, its gesture handed points in pt and gathered into ONE undo step, its overlay drawn', async () => {
      const r = await page.evaluate(() => { const tools = [...document.querySelectorAll('.ce-tools > .ce-tool, .ce-tools > .ce-drop > .ce-tool')].map(b => b.dataset.tool || b.dataset.tip); return tools; });
      assert.ok(r.indexOf('xbox') > r.indexOf('hand') && r.indexOf('xbox') < r.length - 1, 'order: ' + r.join(','));
      await page.keyboard.press('b'); await settle(page);
      const s = await page.evaluate(() => ({ tool: CharmEditor.current.tool(), cur: getComputedStyle(document.querySelector('.ce-stage')).cursor, hint: document.querySelector('[data-hint]').textContent, on: document.querySelector('.ce-tool[data-tool="xbox"]').classList.contains('is-on') }));
      assert.equal(s.tool, 'xbox'); assert.equal(s.cur, 'cell'); assert.equal(s.hint, 'Drag a box (test tool)'); assert.ok(s.on);
      const h0 = await page.evaluate(() => CharmEditor.current.history().undo), n0 = (await page.evaluate(() => CharmEditor.current.items().length));
      const a = await mmClient(page, 4, 4), b = await mmClient(page, 9, 7);
      await page.mouse.move(a.x - 20, a.y - 20); await page.mouse.move(a.x, a.y);
      await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 6 });
      const mid = await page.evaluate(() => ({ n: CharmEditor.current.items().length, overlay: !!document.querySelector('.ce-ui .x-overlay') }));
      await page.mouse.up(); await settle(page);
      const it = await page.evaluate(() => CharmEditor.current.items().slice(-1)[0]);
      const h1 = await page.evaluate(() => CharmEditor.current.history().undo);
      assert.equal(mid.n, n0 + 1, 'the box is in the work while it is drawn'); assert.ok(mid.overlay, 'overlay drawn');
      const bb = await page.evaluate(id => CharmEditor.current.box(id), it.id), A = await mmPt(page, 4, 4), B = await mmPt(page, 9, 7);
      assert.ok(near(bb[0], A[0], 0.6) && near(bb[3], B[1], 0.6), JSON.stringify(bb));
      assert.equal(h1, h0 + 1, 'one undo step for the whole gesture');
      const L = await log(page); assert.ok(L.includes('hover') && L.includes('down') && L.includes('move') && L.includes('up'), L.join(','));
      await page.keyboard.press('q'); assert.ok((await log(page)).includes('key-q'));
      await page.keyboard.press('Control+z'); await settle(page);
      assert.equal(await page.evaluate(() => CharmEditor.current.items().length), n0);
      assert.ok(await page.evaluate(() => document.querySelector('.ce-layers').querySelectorAll('.ce-layer').length === CharmEditor.current.items().length));
    });
    await step('0.3 flyouts: a "shapes" tool joins the Shapes menu; tools naming the same flyout share one rail button whose icon follows the one picked', async () => {
      const r = await page.evaluate(() => ({ shapes: [...document.querySelectorAll('[data-drop="shapes"] .ce-menu [data-tool]')].map(b => b.dataset.tool), pen: [...document.querySelectorAll('[data-drop="x-Pen"] .ce-menu [data-tool]')].map(b => b.dataset.tool) }));
      assert.deepEqual(r.shapes, ['rect', 'ellipse', 'line', 'polygon', 'xstar']); assert.deepEqual(r.pen, ['xpen', 'xpen2']);
      await page.evaluate(() => CharmEditor.current.ctx.setTool('xpen2')); await settle(page);
      const b = await page.evaluate(() => { const btn = document.querySelector('[data-drop="x-Pen"] .ce-drop__b'); return { tool: btn.dataset.tool, on: btn.classList.contains('is-on') }; });
      assert.deepEqual(b, { tool: 'xpen2', on: true });
      await page.keyboard.press('v'); await settle(page);
    });
    await step('0.4 the command: in the Object menu and the right-click menu (off when not enabled), on the bar while enabled, its key; it is one undo step', async () => {
      await page.evaluate(() => CharmEditor.current.ctx.select(['heart-fill'])); await settle(page);
      const s = await page.evaluate(() => ({ bar: [...document.querySelectorAll('.ce-xbar [data-xcmd]')].map(b => b.dataset.xcmd), hidden: document.querySelector('.ce-xbar').hidden }));
      assert.deepEqual(s.bar, ['xnudge']); assert.equal(s.hidden, false);
      await page.click('[data-drop="xcmd"] .ce-drop__b'); await settle(page);
      const menu = await page.evaluate(() => [...document.querySelectorAll('[data-drop="xcmd"] .ce-menu [data-xcmd]')].map(b => b.dataset.xcmd + (b.classList.contains('is-off') ? ':off' : '')));
      assert.deepEqual(menu, ['xnudge', 'xnever:off']);
      const x0 = (await page.evaluate(() => CharmEditor.current.box('heart-fill')))[0], h0 = await page.evaluate(() => CharmEditor.current.history().undo);
      await page.click('[data-drop="xcmd"] [data-xcmd="xnudge"]'); await settle(page);
      const x1 = (await page.evaluate(() => CharmEditor.current.box('heart-fill')))[0];
      assert.ok(near(x1 - x0, 72 / 25.4, 1e-6), `${x1 - x0}`);
      await page.keyboard.press('Control+Shift+K'); await settle(page);
      const x2 = (await page.evaluate(() => CharmEditor.current.box('heart-fill')))[0];
      assert.ok(near(x2 - x0, 2 * 72 / 25.4, 1e-6));
      assert.equal(await page.evaluate(() => CharmEditor.current.history().undo), h0 + 2);
      const hb = await page.evaluate(() => CharmEditor.current.box('heart-fill')), c = await ptClient(page, (hb[0] + hb[2]) / 2, (hb[1] + hb[3]) / 2);
      await page.mouse.click(c.x, c.y, { button: 'right' }); await settle(page);
      const ctxm = await page.evaluate(() => [...document.querySelectorAll('.ce-ctx [data-xcmd]')].map(b => b.dataset.xcmd));
      assert.deepEqual(ctxm, ['xnudge'], 'the right-click menu lists only what can be done');
      await page.keyboard.press('Escape'); await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await settle(page);
    });
    await step('0.5 the importer: the Import button (a picker of every accepted type), a file dropped on the work area (where it fell), a file pasted; each import one undo step', async () => {
      const n0 = await page.evaluate(() => CharmEditor.current.items().length), h0 = await page.evaluate(() => CharmEditor.current.history().undo);
      const [fc] = await Promise.all([page.waitForEvent('filechooser'), page.click('[data-act="import"]')]);
      await fc.setFiles({ name: 'a.png', mimeType: 'image/png', buffer: Buffer.from([137, 80, 78, 71]) });
      await page.waitForFunction(n => CharmEditor.current.items().length === n + 1, n0);
      const at = await mmClient(page, 12, 15);
      await page.evaluate(([x, y]) => {
        const dt = new DataTransfer(); dt.items.add(new File([new Uint8Array([1, 2, 3])], 'b.jpg', { type: 'image/jpeg' }));
        const st = document.querySelector('.ce-stage');
        st.dispatchEvent(new DragEvent('dragover', { bubbles: true, cancelable: true, clientX: x, clientY: y, dataTransfer: dt }));
        window.__dropCue = st.classList.contains('is-drop');
        st.dispatchEvent(new DragEvent('drop', { bubbles: true, cancelable: true, clientX: x, clientY: y, dataTransfer: dt }));
      }, [at.x, at.y]);
      await page.waitForFunction(n => CharmEditor.current.items().length === n + 2, n0);
      await page.evaluate(() => { const dt = new DataTransfer(); dt.items.add(new File([new Uint8Array([1])], 'c.png', { type: 'image/png' })); document.querySelector('.ce-win').dispatchEvent(new ClipboardEvent('paste', { bubbles: true, cancelable: true, clipboardData: dt })); });
      await page.waitForFunction(n => CharmEditor.current.items().length === n + 3, n0);
      const L = (await log(page)).filter(s => s.startsWith('import'));
      assert.deepEqual(L, ['import:a.png:button:-', 'import:b.jpg:drop:at', 'import:c.png:paste:-']);
      assert.ok(await page.evaluate(() => window.__dropCue), 'the work area shows it will take the file');
      assert.equal(await page.evaluate(() => CharmEditor.current.history().undo), h0 + 3);
      await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await settle(page);
      assert.equal(await page.evaluate(() => CharmEditor.current.items().length), n0);
    });
    await step('0.6 the editor\'s own copy still pastes with importers present (⌘C then ⌘V)', async () => {
      await page.evaluate(() => CharmEditor.current.ctx.select(['heart-fill'])); await settle(page);
      const n0 = await page.evaluate(() => CharmEditor.current.items().length);
      await page.keyboard.press('Control+c'); await page.keyboard.press('Control+v');
      await page.waitForFunction(n => CharmEditor.current.items().length === n + 1, n0, { timeout: 3000 });
      await page.keyboard.press('Control+z'); await settle(page);
    });
    await step('0.7 the panel: a head button opens the slot right of the Layers (mounted once, the room narrower, the charm where it was on screen), update follows the selection, it folds away', async () => {
      const before = await page.evaluate(() => ({ w: document.querySelector('.ce-stage').getBoundingClientRect().width, c: CharmEditor.current.clientOf(150, 100) }));
      await page.click('[data-xpanel="xlib"]'); await settle(page); await settle(page);
      const s = await page.evaluate(() => { const p = document.querySelector('.ce-xpanel'), d = document.querySelector('.ce-dock'); const pr = p.getBoundingClientRect(), dr = d.getBoundingClientRect();
        return { shown: !p.hidden, body: !!p.querySelector('.x-panel-body'), right: pr.left >= dr.right - 1, w: document.querySelector('.ce-stage').getBoundingClientRect().width, tab: p.querySelector('[data-xtab="xlib"]').classList.contains('is-on'), c: CharmEditor.current.clientOf(150, 100) }; });
      assert.ok(s.shown && s.body && s.right && s.tab, JSON.stringify(s));
      assert.ok(s.w < before.w - 200, `${before.w} -> ${s.w}`);
      await page.evaluate(() => CharmEditor.current.ctx.select(['heart-line'])); await settle(page);
      assert.ok((await log(page)).includes('update:1'));
      await page.click('[data-act="xpanel0"]'); await settle(page);
      assert.ok(await page.evaluate(() => document.querySelector('.ce-xpanel').hidden));
      await page.click('[data-xpanel="xlib"]'); await settle(page);
      assert.equal((await log(page)).filter(x => x === 'mount').length, 1, 'mounted once');
    });
    await step('0.8 onClose gets the result; panels unmount; no page errors', async () => {
      await page.evaluate(() => document.querySelector('.ce-root [data-act="save"]').click());
      await page.waitForFunction(() => window.__closed === true);
      const L = await log(page); assert.ok(L.includes('close:saved') && L.includes('unmount'), L.join(','));
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }
  {
    const page = await newPage(390, 844);
    await page.evaluate(registerExtensions);
    await openIt(page);
    await step('0.9 at phone width the head keeps every extension button in reach (no overlap, nothing off screen) and the panel folds over the room as the Layers do', async () => {
      const lay = () => page.evaluate(() => {
        const vis = el => { const r = el.getBoundingClientRect(); return r.width > 0.5 && r.height > 0.5 && !el.closest('[hidden]') && getComputedStyle(el).display !== 'none'; };
        const els = [...document.querySelectorAll('.ce-head > *, .ce-head > .ce-drop > .ce-drop__b')].filter(vis).filter(el => !el.classList.contains('ce-drop')), bad = [];
        for (let i = 0; i < els.length; i++) { const a = els[i].getBoundingClientRect(); if (a.left < -0.5 || a.right > innerWidth + 0.5) bad.push('off: ' + els[i].className);
          for (let j = i + 1; j < els.length; j++) { const b = els[j].getBoundingClientRect(); if (Math.min(a.right, b.right) - Math.max(a.left, b.left) > 0.75 && Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top) > 0.75) bad.push(els[i].className + ' / ' + els[j].className); } }
        return { bad, n: els.length };
      });
      const l = await lay();
      assert.deepEqual(l.bad, []); assert.ok(l.n >= 6, 'head controls: ' + l.n);
      await page.click('[data-xpanel="xlib"]'); await settle(page);
      const p = await page.evaluate(() => { const el = document.querySelector('.ce-xpanel'), r = el.getBoundingClientRect(); return { pos: getComputedStyle(el).position, l: r.left, r: r.right, w: innerWidth, body: !!el.querySelector('.x-panel-body') }; });
      assert.ok(p.pos === 'absolute' && p.r <= p.w + 0.5 && p.l >= 0 && p.body, JSON.stringify(p));
      const s = await page.evaluate(() => { const r = document.querySelector('.ce-id h2').getBoundingClientRect(); return { x: r.left + 10, y: r.top + r.height / 2 }; });
      await page.mouse.click(s.x, s.y); await settle(page);
      assert.ok(await page.evaluate(() => document.querySelector('.ce-xpanel').hidden), 'a press anywhere else folds it away');
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }

  const num = t => (t == null || t === '' ? NaN : parseFloat(String(t).replace('−', '-')));
  const sizeIn = t => { const m = /([\d.]+) × ([\d.]+) mm/.exec(t || ''); return m ? [+m[1], +m[2]] : null; };
  async function gesture(page, label, fn) { await takeWork(page); await fn(); record(label, await takeWork(page)); }

  /* ════ 1 · live feedback on the calendar charm ════ */
  {
    const page = await newPage(1280, 720);
    await page.evaluate(instrument);
    await openIt(page);
    const PX = await mmPx(page);
    let rid = null;
    await step('1.1 drawing a rectangle: the chip beside it says W × H and X / Y / W / H follow every frame (they stayed blank); it is drawn where the hand went', async () => {
      if (await snapOn(page)) await page.click('.ce-snapb');   // (exact sizes here: snapping has a part of its own)
      await page.keyboard.press('r'); await settle(page);
      const before = await fields(page);
      assert.equal(before.w, null, 'nothing selected: no size yet');
      const a = await mmClient(page, 3.5, 27.0), b = await mmClient(page, 9.5, 28.4);
      let midChip, midF, endChip, endF;
      await gesture(page, 'calendar · draw a rectangle', async () => {
        await page.mouse.move(a.x, a.y); await page.mouse.down();
        await page.mouse.move(a.x + (b.x - a.x) / 2, a.y + (b.y - a.y) / 2, { steps: 12 });
        midChip = await chip(page); midF = await fields(page);
        await page.mouse.move(b.x, b.y, { steps: 12 });
        endChip = await chip(page); endF = await fields(page);
        await page.mouse.up(); await settle(page);
      });
      const m = sizeIn(midChip), e = sizeIn(endChip);
      assert.ok(m && near(m[0], 3, 0.05) && near(m[1], 0.7, 0.05), 'mid-drag chip: ' + midChip);
      assert.ok(near(num(midF.x), 3.5, 0.05) && near(num(midF.y), 27, 0.05) && near(num(midF.w), 3, 0.05) && near(num(midF.h), 0.7, 0.05) && midF.r === '0', 'mid-drag fields: ' + JSON.stringify(midF));
      assert.ok(e && near(e[0], 6, 0.05) && near(e[1], 1.4, 0.05), 'chip at the end: ' + endChip);
      assert.ok(near(num(endF.w), 6, 0.05) && near(num(endF.h), 1.4, 0.05), JSON.stringify(endF));
      rid = await lastId(page);
      const bb = await boxMm(page, rid), sel = await page.evaluate(() => CharmEditor.current.selection());
      assert.ok(near(bb[0], 3.5, 0.05) && near(bb[1], 27, 0.05) && near(bb[2], 9.5, 0.05) && near(bb[3], 28.4, 0.05), JSON.stringify(bb));
      assert.deepEqual(sel, [rid]);
      const after = await fields(page);
      assert.ok(near(num(after.w), 6, 0.05) && near(num(after.x), 3.5, 0.05), JSON.stringify(after));
      console.log(`         mid-drag: chip "${midChip}", fields X ${midF.x} Y ${midF.y} W ${midF.w} H ${midF.h}; at the end "${endChip}"`);
    });
    await step('1.2 a line says its length and angle; a polygon its sides (↑ while dragging adds one); Esc mid-drag leaves nothing and the fields as they were', async () => {
      await page.keyboard.press('l'); await settle(page);
      const a = await mmClient(page, 12, 27.2), b = await mmClient(page, 16, 28.2);
      await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 8 });
      const lc = await chip(page);
      const n0 = await page.evaluate(() => CharmEditor.current.items().length);
      await page.keyboard.press('Escape'); await page.mouse.up(); await settle(page);
      const m = /([\d.]+) mm · ([\d.-]+)°/.exec(lc || '');
      assert.ok(m && near(+m[1], Math.hypot(4, 1), 0.06) && near(+m[2], Math.atan2(1, 4) * 180 / Math.PI, 0.6), 'line chip: ' + lc);
      assert.equal(await page.evaluate(() => CharmEditor.current.items().length), n0);
      assert.ok(near(num((await fields(page)).w), 6, 0.05), 'the fields are the selection\'s again');
      await page.evaluate(() => CharmEditor.current.ctx.setTool('polygon')); await settle(page);
      const c = await mmClient(page, 13, 27.0), d = await mmClient(page, 17, 28.5);
      await page.mouse.move(c.x, c.y); await page.mouse.down(); await page.mouse.move(d.x, d.y, { steps: 8 });
      const p6 = await chip(page); await page.keyboard.press('ArrowUp'); const p7 = await chip(page);
      await page.mouse.up(); await settle(page);
      const it = await page.evaluate(() => CharmEditor.current.items().slice(-1)[0]);
      assert.ok(/6 sides/.test(p6) && /7 sides/.test(p7), p6 + ' / ' + p7);
      assert.equal(it.name, 'Polygon'); assert.equal(it.subpaths[0].filter(o => o[0] === 'l').length, 6, '7 corners');
      console.log(`         line "${lc}" · polygon "${p6}" then "${p7}"`);
      await page.keyboard.press('Control+z'); await settle(page);
      await page.evaluate(id => CharmEditor.current.ctx.select([id]), rid); await settle(page);
    });
    const centre = bb => [(bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2];
    async function handleDrag(label, h, dxMm, dyMm, keys, mid) {
      const p = await handleAt(page, h); assert.ok(p, 'handle ' + h);
      let out = {};
      await gesture(page, label, async () => {
        for (const k of keys) await page.keyboard.down(k);
        await page.mouse.move(p.x, p.y); await page.mouse.down();
        await page.mouse.move(p.x + dxMm * PX / 2, p.y + dyMm * PX / 2, { steps: 10 });
        if (mid) out = await mid();
        await page.mouse.move(p.x + dxMm * PX, p.y + dyMm * PX, { steps: 10 });
        await page.mouse.up();
        for (const k of keys.slice().reverse()) await page.keyboard.up(k);
        await settle(page);
      });
      return out;
    }
    await step('1.3 Ctrl-drag of a corner resizes about the centre (the chip and the fields live, the cursor the handle\'s); Alt does too; Shift keeps the proportions; Ctrl + Shift both', async () => {
      const b0 = await boxMm(page, rid), c0 = centre(b0);
      const mid = await handleDrag('calendar · resize (Ctrl)', 'se', 1, 0.5, ['Control'], async () => ({ chip: await chip(page), f: await fields(page), cur: await page.evaluate(() => getComputedStyle(document.querySelector('.ce-stage')).cursor) }));
      const b1 = await boxMm(page, rid), c1 = centre(b1);
      assert.ok(near(b1[2] - b1[0], b0[2] - b0[0] + 2, 0.002) && near(b1[3] - b1[1], b0[3] - b0[1] + 1, 0.002), 'W 6 -> 8, H 1.4 -> 2.4 (twice the hand: both sides move): ' + JSON.stringify(b1));
      assert.ok(near(c1[0], c0[0], 1e-6) && near(c1[1], c0[1], 1e-6), 'the centre stayed: ' + c0 + ' -> ' + c1);
      const mc = sizeIn(mid.chip); assert.ok(mc && near(mc[0], 7, 0.06) && near(mc[1], 1.9, 0.06), 'mid-drag chip ' + mid.chip);
      assert.ok(near(num(mid.f.w), 7, 0.06) && near(num(mid.f.h), 1.9, 0.06), 'mid-drag fields ' + JSON.stringify(mid.f));
      assert.equal(mid.cur, 'nwse-resize');
      await handleDrag('calendar · resize (Alt)', 'e', 0.5, 0, ['Alt']);
      const b2 = await boxMm(page, rid), c2 = centre(b2);
      assert.ok(near(b2[2] - b2[0], b1[2] - b1[0] + 1, 0.002) && near(c2[0], c0[0], 1e-6), 'Alt about the centre too: ' + JSON.stringify(b2));
      await handleDrag('calendar · resize (Shift)', 'se', 2, 0.2, ['Shift']);
      const b3 = await boxMm(page, rid);
      const ratio = (b2[2] - b2[0]) / (b2[3] - b2[1]);
      assert.ok(near((b3[2] - b3[0]) / (b3[3] - b3[1]), ratio, 1e-6) && near(b3[0], b2[0], 1e-6) && near(b3[1], b2[1], 1e-6) && near(b3[2] - b3[0], b2[2] - b2[0] + 2, 0.002), 'Shift: the ratio and the far corner kept: ' + JSON.stringify(b3));
      await handleDrag('calendar · resize (Ctrl+Shift)', 'se', 1, 0, ['Control', 'Shift']);
      const b4 = await boxMm(page, rid), c4 = centre(b4), c3 = centre(b3);
      assert.ok(near((b4[2] - b4[0]) / (b4[3] - b4[1]), ratio, 1e-6) && near(c4[0], c3[0], 1e-6) && near(c4[1], c3[1], 1e-6) && near(b4[2] - b4[0], b3[2] - b3[0] + 2, 0.002), 'both: ' + JSON.stringify(b4));
      console.log(`         W × H: 6.00 × 1.40 -> Ctrl ${(b1[2] - b1[0]).toFixed(2)} × ${(b1[3] - b1[1]).toFixed(2)} (centre moved ${Math.hypot(c1[0] - c0[0], c1[1] - c0[1]).toExponential(1)} mm) -> Alt ${(b2[2] - b2[0]).toFixed(2)} -> Shift ${(b3[2] - b3[0]).toFixed(2)} × ${(b3[3] - b3[1]).toFixed(2)} -> Ctrl+Shift ${(b4[2] - b4[0]).toFixed(2)} × ${(b4[3] - b4[1]).toFixed(2)}; mid-drag chip "${mid.chip}"`);
    });
    await step('1.4 Ctrl pressed and let go mid-drag counts at once, without moving the mouse', async () => {
      const b0 = await boxMm(page, rid), p = await handleAt(page, 'e');
      await page.mouse.move(p.x, p.y); await page.mouse.down(); await page.mouse.move(p.x + PX, p.y, { steps: 6 });
      const a = await boxMm(page, rid);
      await page.keyboard.down('Control'); await settle(page); const b = await boxMm(page, rid);
      await page.keyboard.up('Control'); await settle(page); const c = await boxMm(page, rid);
      await page.mouse.up(); await settle(page);
      assert.ok(near(a[0], b0[0], 1e-6) && near(a[2], b0[2] + 1, 0.002), 'plain: the left edge stays ' + JSON.stringify(a));
      assert.ok(near(b[0], b0[0] - 1, 0.002) && near(b[2], b0[2] + 1, 0.002), 'Ctrl down: about the centre at once ' + JSON.stringify(b));
      assert.ok(near(c[0], b0[0], 1e-6), 'Ctrl up: the left edge back ' + JSON.stringify(c));
      await page.keyboard.press('Control+z'); await settle(page);
    });
    await step('1.5 turning: the chip and the angle field follow (Shift: 15°); then the cursors turn with the part (resize arrows, a turn cursor, turn zones outside the corners)', async () => {
      const bb = await page.evaluate(id => CharmEditor.current.box(id), rid), cc = await ptClient(page, (bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2);
      const p = await handleAt(page, 'rot'), v = [p.x - cc.x, p.y - cc.y], t = 45 * Math.PI / 180;
      const q = { x: cc.x + v[0] * Math.cos(t) - v[1] * Math.sin(t), y: cc.y + v[0] * Math.sin(t) + v[1] * Math.cos(t) };
      let midChip, midR, cur;
      await gesture(page, 'calendar · turn', async () => {
        await page.keyboard.down('Shift');
        await page.mouse.move(p.x, p.y); await page.mouse.down();
        await page.mouse.move((p.x + q.x) / 2, (p.y + q.y) / 2, { steps: 10 });
        midChip = await chip(page); midR = (await fields(page)).r; cur = await page.evaluate(() => getComputedStyle(document.querySelector('.ce-stage')).cursor);
        await page.mouse.move(q.x, q.y, { steps: 10 }); await page.mouse.up(); await page.keyboard.up('Shift'); await settle(page);
      });
      assert.ok(/^-?\d+(\.\d)?°$/.test(midChip || '') && num(midR) === num(midChip), 'mid-turn chip and field agree: ' + midChip + ' / ' + midR);
      assert.ok(/url\(/.test(cur), 'the turn cursor while turning: ' + cur.slice(0, 40));
      assert.equal((await fields(page)).r, '45');
      const c = await page.evaluate(() => { const g = h => document.querySelector(`.ce-ui .u-h[data-h="${h}"]`); return { e: g('e').style.cursor, n: g('n').style.cursor, se: g('se').style.cursor, rot: getComputedStyle(g('rot')).cursor, zones: document.querySelectorAll('.ce-ui .u-rz').length }; });
      assert.deepEqual([c.e, c.n, c.se], ['nwse-resize', 'nesw-resize', 'ns-resize']);
      assert.ok(/url\(/.test(c.rot)); assert.equal(c.zones, 4);
      console.log(`         mid-turn "${midChip}" (field ${midR}); at 45°: e ${c.e}, n ${c.n}, se ${c.se}`);
      await page.keyboard.press('Control+z'); await settle(page);
    });
    await step('1.6 moving: the chip says X and Y and the fields follow; the cursor says move; the arrow keys\' nudge is shown (where it is and how far the run went)', async () => {
      const bb = await page.evaluate(id => CharmEditor.current.box(id), rid), cc = await ptClient(page, bb[0] + (bb[2] - bb[0]) * 0.25, bb[1]);   // (on its top line, between two handles)
      const b0 = await boxMm(page, rid);
      let midChip, midF, cur;
      await gesture(page, 'calendar · move', async () => {
        await page.mouse.move(cc.x, cc.y); await page.mouse.down();
        await page.mouse.move(cc.x + PX / 2, cc.y, { steps: 10 });
        midChip = await chip(page); midF = await fields(page); cur = await page.evaluate(() => getComputedStyle(document.querySelector('.ce-stage')).cursor);
        await page.mouse.move(cc.x + PX, cc.y, { steps: 10 }); await page.mouse.up(); await settle(page);
      });
      const m = /X ([\d.]+)\s+Y ([\d.]+) mm/.exec(midChip || '');
      assert.ok(m && near(+m[1], b0[0] + 0.5, 0.06) && near(num(midF.x), +m[1], 0.006), 'mid-move chip and X field: ' + midChip + ' / ' + midF.x);
      assert.equal(cur, 'move');
      const x0 = num((await fields(page)).x);
      for (let i = 0; i < 4; i++) await page.keyboard.press('ArrowRight');
      await settle(page);
      const nc = await chip(page), x1 = num((await fields(page)).x);
      assert.ok(/\+0\.20, \+0\.00/.test(nc || ''), 'the nudge shown: ' + nc);
      assert.ok(near(x1 - x0, 0.2, 1e-6), `X ${x0} -> ${x1}`);
      console.log(`         mid-move "${midChip}"; after four taps of → "${nc}"`);
    });
    await step('1.7 hover lights the part under the cursor (and the part of a Layers row under it), the cursor says it can be moved; the marquee selects live as it sweeps', async () => {
      await page.keyboard.press('Escape'); await settle(page);
      const e = await page.evaluate(() => { const b = CharmEditor.current.box('day-1-1'); return CharmEditor.current.clientOf(b[0], (b[1] + b[3]) / 2); });
      await gesture(page, 'calendar · hover', async () => {
        const s0 = await mmClient(page, 2, 14);
        await page.mouse.move(s0.x, s0.y); await page.mouse.move(e.x, e.y, { steps: 30 }); await settle(page);
      });
      const h = await page.evaluate(() => ({ hover: document.querySelectorAll('.ce-ui .u-hover').length, cur: getComputedStyle(document.querySelector('.ce-stage')).cursor }));
      assert.ok(h.hover >= 1 && h.cur === 'move', JSON.stringify(h));
      const row = await page.evaluate(() => { const r = document.querySelector('.ce-layer[data-id="heart-line"]').getBoundingClientRect(); return { x: r.left + 60, y: r.top + r.height / 2 }; });
      await page.mouse.move(row.x, row.y); await settle(page);
      assert.ok(await page.evaluate(() => document.querySelectorAll('.ce-ui .u-hover').length >= 1), 'a row lights its part');
      const a = await mmClient(page, 2.0, 9.6), b = await mmClient(page, 6.2, 12.6);
      let midSel = 0, marq = false;
      await gesture(page, 'calendar · marquee', async () => {
        await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 20 });
        midSel = (await page.evaluate(() => CharmEditor.current.selection())).length; marq = await page.evaluate(() => !!document.querySelector('.ce-ui .u-marq'));
        await page.mouse.up(); await settle(page);
      });
      assert.ok(marq && midSel >= 2, `marquee drawn ${marq}, ${midSel} selected while sweeping`);
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }

  /* ════ 2 · smart guides on a plain charm ════ */
  {
    const page = await newPage(1280, 720);
    await page.evaluate(f => { window.openEditor({ faces: { front: f } }); }, plainFace([['A', 5, 5, 9, 9], ['C', 25, 5, 29, 9], ['B', 14, 20, 18, 24]]));
    await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page);
    const PX = await mmPx(page);
    const dragPart = async (id, dxMm, dyMm, mid) => {
      const b = await page.evaluate(id => CharmEditor.current.box(id), id), c = await ptClient(page, b[0], b[1] + (b[3] - b[1]) * 0.25);   // (on its left line, between two handles)
      await page.mouse.move(c.x, c.y); await page.mouse.down();
      await page.mouse.move(c.x + dxMm * PX, c.y + dyMm * PX, { steps: 16 });
      const out = mid ? await mid() : null;
      await page.mouse.up(); await settle(page); return out;
    };
    const ui = () => page.evaluate(() => ({ guides: document.querySelectorAll('.ce-ui .u-gd').length, ink: [...document.querySelectorAll('.ce-ui .u-bdg--ink text')].map(t => t.textContent), gap: [...document.querySelectorAll('.ce-ui .u-bdg--gap text')].map(t => t.textContent) }));
    await step('2.1 a part dragged near another\'s edge snaps onto it exactly, the guide drawn and the gap between them measured on it', async () => {
      assert.ok(await snapOn(page), 'Snap starts on');
      const mid = await dragPart('B', 9.15 - 14, 0, ui);
      const b = await boxMm(page, 'B');
      assert.ok(near(b[0], 9, 1e-6), 'left edge on A\'s right edge: ' + b[0]);
      assert.ok(mid.guides >= 1 && mid.ink.includes('11.00'), JSON.stringify(mid));
      console.log(`         let go at 9.15 mm, snapped to ${b[0].toFixed(6)} mm; ${mid.guides} guides, gaps shown ${mid.ink.join(', ')}`);
    });
    await step('2.2 dropped between two parts in a row, it snaps to even spacing and both gaps are shown', async () => {
      const mid = await dragPart('B', 15.2 - 9, 5 - 20, ui);
      const b = await boxMm(page, 'B');
      assert.ok(near(b[0], 15, 1e-6) && near(b[1], 5, 1e-6), 'even: ' + JSON.stringify(b));
      assert.deepEqual(mid.gap, ['6.00', '6.00']);
      console.log(`         let go at 15.20 mm, placed at ${b[0].toFixed(6)} mm: gaps ${mid.gap.join(' and ')} mm`);
    });
    await step('2.3 the Snap switch turns snapping off (nothing pulls, no guides) and ⌘U on again', async () => {
      await page.click('.ce-snapb'); assert.equal(await snapOn(page), false);
      const mid = await dragPart('B', 9.15 - 15, 0, ui);
      const b = await boxMm(page, 'B');
      assert.ok(near(b[0], 9.15, 0.06) && Math.abs(b[0] - 9) > 0.08 && mid.guides === 0, JSON.stringify({ b, mid }));
      await page.keyboard.press('Control+u'); await settle(page);
      assert.equal(await snapOn(page), true);
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }

  /* ════ 3 · frame work on a 300-path charm ════ */
  {
    const page = await newPage(1280, 720);
    await page.evaluate(instrument);
    await page.evaluate(f => { window.openEditor({ faces: { front: f } }); }, busyFace());
    await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page);
    const PX = await mmPx(page);
    await step('3.1 hover, draw, move, resize, turn, marquee and everything moved at once on 300 parts', async () => {
      assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-items path').length), 301);
      const P = (x, y) => mmClient(page, x, y);
      await gesture(page, '300 parts · hover', async () => { const a = await P(2, 2), b = await P(37, 36); await page.mouse.move(a.x, a.y); await page.mouse.move(b.x, b.y, { steps: 60 }); await settle(page); });
      await page.keyboard.press('r');
      await gesture(page, '300 parts · draw', async () => { const a = await P(5, 6), b = await P(25, 30); await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 40 }); await page.mouse.up(); await settle(page); });
      await page.keyboard.press('Control+z'); await page.evaluate(() => CharmEditor.current.ctx.select(['p150'])); await settle(page);
      const b = await page.evaluate(() => CharmEditor.current.box('p150')), c = await ptClient(page, (b[0] + b[2]) / 2, (b[1] + b[3]) / 2);
      await gesture(page, '300 parts · move one', async () => { await page.mouse.move(c.x, c.y); await page.mouse.down(); await page.mouse.move(c.x + 3 * PX, c.y + 2 * PX, { steps: 40 }); await page.mouse.up(); await settle(page); });
      const se = await handleAt(page, 'se');
      await gesture(page, '300 parts · resize one (Ctrl)', async () => { await page.keyboard.down('Control'); await page.mouse.move(se.x, se.y); await page.mouse.down(); await page.mouse.move(se.x + 2 * PX, se.y + PX, { steps: 40 }); await page.mouse.up(); await page.keyboard.up('Control'); await settle(page); });
      const rot = await handleAt(page, 'rot');
      await gesture(page, '300 parts · turn one', async () => { await page.mouse.move(rot.x, rot.y); await page.mouse.down(); await page.mouse.move(rot.x + 3 * PX, rot.y + 2 * PX, { steps: 40 }); await page.mouse.up(); await settle(page); });
      await page.keyboard.press('Escape');
      await gesture(page, '300 parts · marquee', async () => { const a = await P(3.3, 4.15), z = await P(20, 20); await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(z.x, z.y, { steps: 40 }); await page.mouse.up(); await settle(page); });
      await page.keyboard.press('Control+a'); await settle(page);
      assert.equal((await page.evaluate(() => CharmEditor.current.selection())).length, 300);
      const q = await page.evaluate(() => CharmEditor.current.box('p21')), qc = await ptClient(page, (q[0] + q[2]) / 2, (q[1] + q[3]) / 2);
      await gesture(page, '300 parts · move all 300', async () => { await page.mouse.move(qc.x, qc.y); await page.mouse.down(); await page.mouse.move(qc.x + 2 * PX, qc.y + PX, { steps: 40 }); await page.mouse.up(); await settle(page); });
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }
  await step('3.2 every gesture\'s frame work stays inside a frame (16 ms), on the calendar charm and on 300 parts', async () => {
    assert.ok(perf.length >= 14, 'measured ' + perf.length);
    for (const m of perf) withinFrame(m, m.label);
    const worst = perf.reduce((a, m) => (m.f.p95 > a.f.p95 ? m : a));
    console.log(`         worst p95: ${worst.label} ${worst.f.p95.toFixed(2)} ms (machine load ${require('os').loadavg()[0].toFixed(1)})`);
  });

  await browser.close();
  console.log(`\nce-feedback: ${results.length} passed, ${failures.length} failed`);
  if (failures.length) { failures.forEach(f => console.log('  - ' + f)); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
