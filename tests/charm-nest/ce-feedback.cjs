// LIVE FEEDBACK AND CONTROL IN THE CHARM EDITOR (charm-nest-charm-editor.js; Paul, 28 Oct 2026: "when I'm adding a circle or a square or any other primitive and then resizing it,
// I'm not seeing a real time read out of the size in the work area of the actually selected object and in the X and Y in the menu ... the ability to resize every object from its
// center-point ... by holding the control key while dragging"). One real Chromium page with nothing but the editor and the calendar charm (fixtures/charm-editor-harness.html);
// no server, no network. Run through the shared slot queue:
//   $SP/int/slot.sh node tests/charm-nest/ce-feedback.cjs
// 0 the extension points the Design & Engrave files plug into: a tool on the rail (its own and in the Shapes flyout), its gesture one undo step; a command in the Object
//   menu, the right-click menu, the bar and its key; an importer reached by the Import button, a dropped file and a pasted one; a panel beside the Layers; onOpen / onClose
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

/* the stand-in extensions: what a file such as charm-nest-ce-paths.js registers */
function registerExtensions() {
  const C = window.CharmEditor, log = window.__xlog = [];
  const rectOf = (a, b) => [[['m', [a.x, a.y]], ['l', [b.x, a.y]], ['l', [b.x, b.y]], ['l', [a.x, b.y]], ['h']]];
  C.tools.push({
    id: 'xbox', title: 'Box maker', key: 'b', cursor: 'cell', tip: 'Drags a box', hint: 'Drag a box (test tool)',
    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><rect x="5" y="5" width="14" height="14"/></svg>',
    onDown(ctx, ev, p) { this.a = p; this.id = ctx.addItems({ kind: 'path', role: 'line', name: 'X box', stroke: true, subpaths: rectOf(p, p) })[0]; log.push('down'); },
    onMove(ctx, ev, p) { if (!this.a || !ev.buttons) { log.push('hover'); return; } ctx.replaceItems({ [this.id]: Object.assign({}, ctx.entry(this.id).it, { subpaths: rectOf(this.a, p) }) }); log.push('move'); },
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
      const mid = await page.evaluate(() => ({ n: CharmEditor.current.items().length, overlay: !!document.querySelector('.ce-ui .x-overlay'), ui: document.querySelector('.ce-ui').innerHTML.slice(-200) + ' tool=' + CharmEditor.current.tool() + ' ov=' + typeof CharmEditor.tools[0].overlay + ' ' + CharmEditor.tools.map(t => t.id) }));
      await page.mouse.up(); await settle(page);
      const it = await page.evaluate(() => CharmEditor.current.items().slice(-1)[0]);
      const h1 = await page.evaluate(() => CharmEditor.current.history().undo);
      assert.equal(mid.n, n0 + 1, 'the box is in the work while it is drawn'); assert.ok(mid.overlay, 'overlay drawn ' + mid.ui);
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
      assert.deepEqual(r.shapes, ['rect', 'ellipse', 'line', 'xstar']); assert.deepEqual(r.pen, ['xpen', 'xpen2']);
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

  await browser.close();
  console.log(`\nce-feedback: ${results.length} passed, ${failures.length} failed`);
  if (failures.length) { failures.forEach(f => console.log('  - ' + f)); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
