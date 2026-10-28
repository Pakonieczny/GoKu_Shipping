// THE CHARM EDITOR AS ILLUSTRATOR (charm-nest-ce-ai-shell.js and the workspace of charm-nest-charm-editor.js; Paul, 28 Oct 2026: "replicate the UI for illustrator while also keeping
// some of the specific functions that are dedicated to our platform"). One real Chromium page: the editor's harness (fixtures/charm-editor-harness.html) with every extension file
// the page loads (ops, paths, warp, extras, trace, image, import), no server, no network.
//   PW_DIR=/opt/node-tools/node_modules CHROMIUM=/opt/pw-browsers/chromium node tests/charm-nest/ce-ai-shell.cjs     (SHOTS=<dir> keeps screenshots at 1440 × 900 and 1024 × 768)
// 1 the menu bar: Illustrator's menus in Illustrator's order, each with Illustrator's names, separators, submenus and the shortcut column (Windows spelling here, the Mac glyphs
//   from the same catalog); greyed only where Illustrator greys; a submenu opens beside its row; F10 and the arrows walk it; items run the editor's own commands
// 2 the Tools panel: Illustrator's order and groups, the extension tools in their places, flyouts, the rich tool tip (name and key), one or two columns with the »
// 3 the Control bar: the selection's kind, Fill / Stroke, Character for words (font, style, size), the reference point and X Y W H
// 4 the document tabs (Front, Back), the status bar (zoom, the artboard navigator, the tool), Fill / Stroke in the Tools panel (Shift+X swap)
// 5 the theme: Preferences ▸ User Interface brightness (Dark, Medium Dark, Medium Light, Light) and Canvas Color, kept per person; screenshots
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const pwDir = process.env.PW_DIR || path.join(__dirname, '../../node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const ROOT = path.join(__dirname, '../..'), HARNESS = 'file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html');
const EXT = ['charm-nest-vector.js', 'charm-nest-ce-ops.js', 'charm-nest-ce-paths.js', 'charm-nest-ce-warp.js', 'charm-nest-ce-extras.js', 'charm-nest-ce-trace.js', 'charm-nest-ce-image.js', 'charm-nest-dxf.js', 'charm-nest-ce-import.js'];
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n         ')); };
async function step(name, fn) { try { const note = await fn(); pass(name + (note ? '  [' + note + ']' : '')); } catch (e) { fail(name, e); } }
const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium', args: ['--no-sandbox'] });
  const newPage = async (w, h, storage) => {
    const ctx = await browser.newContext({ viewport: { width: w, height: h }, reducedMotion: 'reduce' });
    await ctx.route(u => !u.href.startsWith('file://'), r => r.abort());
    const page = await ctx.newPage();
    page.__errs = [];
    page.on('pageerror', e => page.__errs.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') page.__errs.push('console: ' + m.text()); });
    await page.goto(HARNESS);
    if (storage) await page.evaluate(s => { for (const [k, v] of Object.entries(s)) localStorage.setItem(k, v); }, storage);
    for (const s of EXT) await page.addScriptTag({ url: 'file://' + path.join(ROOT, s) });
    return page;
  };
  const openIt = async (page, extra) => { await page.evaluate(x => { window.openEditor(x || undefined); }, extra || null); await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page); };
  const shot = async (page, name) => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name) }); };
  const C = (page, fn, arg) => page.evaluate(([f, a]) => (0, eval)('(' + f + ')')(window.CharmEditor.current, a), [String(fn), arg]);
  const closeM = page => page.evaluate(() => window.CharmEditor.current.ai.closeMenus());
  const menuOpen = async (page, name) => { await closeM(page); await page.click(`.ce-menubar .ce-mb__t:text-is("${name}")`); await settle(page); };
  const rows = page => page.evaluate(() => { const m = document.querySelector('.ce-aimenu:not([hidden])'); return m ? [...m.querySelectorAll(':scope > .ce-aimi, :scope > .ce-aisub > .ce-aimi, :scope > .ce-menu__sep')].map(b => b.classList.contains('ce-menu__sep') ? '-' : (b.querySelector('.ce-aimi__l') || b).textContent + (b.querySelector('em') ? ' [' + b.querySelector('em').textContent + ']' : '') + (b.classList.contains('is-off') ? ' (off)' : '') + (b.classList.contains('ce-aimi--sub') ? ' ▸' : '') + (b.classList.contains('is-chk') ? ' ✓' : '')) : null; });
  const subRows = async (page, label) => {
    await page.hover(`.ce-aimenu:not([hidden]) .ce-aimi--sub:has(.ce-aimi__l:text-is("${label}"))`); await settle(page);
    return page.evaluate(l => { const b = [...document.querySelectorAll('.ce-aimenu .ce-aimi--sub')].find(x => x.querySelector('.ce-aimi__l').textContent === l); const m = b && b.parentNode.querySelector(':scope > .ce-menu'); return m && !m.hidden ? [...m.querySelectorAll(':scope > .ce-aimi, :scope > .ce-aisub > .ce-aimi')].map(x => x.querySelector('.ce-aimi__l').textContent + (x.querySelector('em') ? ' [' + x.querySelector('em').textContent + ']' : '') + (x.classList.contains('is-off') ? ' (off)' : '')) : null; }, label);
  };
  const pick = async (page, label) => { await page.locator(`.ce-aimenu:not([hidden]) .ce-aimi:visible:has(.ce-aimi__l:text-is("${label}"))`).last().click(); await settle(page); };
  const sel = (page, ids) => page.evaluate(ids => window.CharmEditor.current.ctx.select(ids), ids).then(() => settle(page));
  const clickPart = async (page, id, mods) => { const c = await C(page, (E, id) => { const b = E.box(id); return E.clientOf((b[0] + b[2]) / 2, (b[1] + b[3]) / 2); }, id); for (const k of mods || []) await page.keyboard.down(k); await page.mouse.click(c.x, c.y); for (const k of mods || []) await page.keyboard.up(k); await settle(page); };

  console.log('1 · the menu bar');
  const page = await newPage(1440, 900);
  await openIt(page);
  await step('1.1 the menu bar is Illustrator\'s: File Edit Object Type Select Effect View Window Help, a menubar of menuitems', async () => {
    const r = await page.evaluate(() => { const b = document.querySelector('.ce-appbar .ce-menubar'); return { role: b.getAttribute('role'), t: [...b.querySelectorAll('.ce-mb__t')].map(x => x.textContent), roles: [...b.querySelectorAll('.ce-mb__t')].every(x => x.getAttribute('role') === 'menuitem' && x.getAttribute('aria-haspopup') === 'true') }; });
    assert.equal(r.role, 'menubar'); assert.ok(r.roles);
    assert.deepEqual(r.t, ['File', 'Edit', 'Object', 'Type', 'Select', 'Effect', 'View', 'Window', 'Help']);
    assert.ok(await page.evaluate(() => !!window.CharmEditor.ai && Array.isArray(window.CharmEditor.ai.catalog) && window.CharmEditor.ai.catalog.length > 80), 'CharmEditor.ai.catalog');
  });
  await step('1.2 Edit: Illustrator\'s items, groups and shortcut column (Windows here: Shift+Ctrl+Z, Alt+Shift+Ctrl+K); Undo and Paste greyed while there is nothing to undo or paste', async () => {
    await menuOpen(page, 'Edit');
    const r = await rows(page);
    assert.deepEqual(r.slice(0, 13), ['Undo [Ctrl+Z] (off)', 'Redo [Shift+Ctrl+Z] (off)', '-', 'Cut [Ctrl+X] (off)', 'Copy [Ctrl+C] (off)', 'Paste [Ctrl+V] (off)', 'Paste in Front [Ctrl+F] (off)', 'Paste in Back [Ctrl+B] (off)', 'Paste in Place [Shift+Ctrl+V] (off)', 'Clear [Del] (off)', 'Duplicate (off)', '-', 'Keyboard Shortcuts… [Alt+Shift+Ctrl+K]']);
    assert.ok(r.includes('Preferences ▸'), r.join(' | '));
    assert.deepEqual(await subRows(page, 'Preferences'), ['General… [Ctrl+K]', 'User Interface…', 'Guides & Grid…']);
    await shot(page, 'ai-menu-edit-1440.png');
    await page.keyboard.press('Escape');
    const mac = await page.evaluate(() => { const A = window.CharmEditor.ai; return [A.keyLabel('Mod+Alt+Shift+8', true), A.keyLabel('Mod+Shift+Z', true), A.keyLabel('Mod++', true), A.keyLabel('Mod+Alt+Shift+8', false), A.keyLabel('Shift+X', false)]; });
    assert.deepEqual(mac, ['⌥⇧⌘8', '⇧⌘Z', '⌘+', 'Alt+Shift+Ctrl+8', 'Shift+X']);
  });
  await step('1.3 Object: Transform ▸ Arrange ▸ Align ▸ | Group Ungroup Lock ▸ Unlock All Hide ▸ Show All | Path ▸ Shape ▸ Blend ▸ Envelope Distort ▸ Image Trace ▸ | Clipping Mask ▸ Compound Path ▸ (our platform\'s in Illustrator\'s places)', async () => {
    await clickPart(page, 'heart-fill');
    await menuOpen(page, 'Object');
    const r = await rows(page);
    const want = ['Transform ▸', 'Arrange ▸', 'Align ▸', '-', 'Group [Ctrl+G] (off)', 'Ungroup [Shift+Ctrl+G] (off)', 'Lock ▸', 'Unlock All [Alt+Ctrl+2]', 'Hide ▸', 'Show All [Alt+Ctrl+3]', '-', 'Path ▸', 'Shape ▸', 'Blend ▸', 'Envelope Distort ▸', 'Image Trace ▸', '-', 'Clipping Mask ▸', 'Compound Path ▸'];
    assert.deepEqual(r.slice(0, want.length).map(x => x.replace(/ \(off\) ▸$/, ' ▸')), want);
    const tr = await subRows(page, 'Transform');
    for (const w of ['Transform Again [Ctrl+D] (off)', 'Move… [Shift+Ctrl+M]', 'Rotate…', 'Reflect…', 'Scale…', 'Flip Horizontal', 'Flip Vertical']) assert.ok(tr.includes(w), w + ' in ' + tr.join(' | '));
    assert.deepEqual(await subRows(page, 'Arrange'), ['Bring to Front [Shift+Ctrl+]]', 'Bring Forward [Ctrl+]]', 'Send Backward [Ctrl+[]', 'Send to Back [Shift+Ctrl+[]']);
    assert.deepEqual(await subRows(page, 'Clipping Mask'), ['Make (Clip to the Charm) [Ctrl+7]', 'Release [Alt+Ctrl+7] (off)']);
    const p = await subRows(page, 'Path');
    for (const w of ['Join [Ctrl+J]', 'Outline Stroke', 'Offset Path…', 'Cut Line Round the Art…', 'Simplify…']) assert.ok(p.some(x => x.startsWith(w)), w + ' in ' + p.join(' | '));
    assert.deepEqual(await subRows(page, 'Shape'), ['Jump Ring on Top']);
    await subRows(page, 'Arrange');
    await shot(page, 'ai-menu-object-1440.png');
    await closeM(page);
  });
  await step('1.4 a menu item runs the editor\'s own command: Object ▸ Arrange ▸ Send to Back puts the heart under everything (one undo step), Edit ▸ Undo puts it back', async () => {
    const z0 = await C(page, E => E.items().map(i => i.id).indexOf('heart-fill')), h0 = await C(page, E => E.history().undo);
    await menuOpen(page, 'Object'); await subRows(page, 'Arrange'); await pick(page, 'Send to Back');
    assert.equal(await C(page, E => E.items().map(i => i.id).indexOf('heart-fill')), 0);
    assert.equal(await C(page, E => E.history().undo), h0 + 1);
    await menuOpen(page, 'Edit'); await pick(page, 'Undo');
    assert.equal(await C(page, E => E.items().map(i => i.id).indexOf('heart-fill')), z0);
  });
  await step('1.5 Edit ▸ Copy, then Paste in Front and Paste in Back: a copy on top of everything and one under everything', async () => {
    const n0 = await C(page, E => E.items().length);
    await menuOpen(page, 'Edit'); await pick(page, 'Copy');
    await menuOpen(page, 'Edit'); assert.ok((await rows(page)).includes('Paste in Front [Ctrl+F]')); await pick(page, 'Paste in Front');
    const top = await C(page, E => { const its = E.items(); return { n: its.length, sel: E.selection(), last: its[its.length - 1].id }; });
    assert.equal(top.n, n0 + 1); assert.deepEqual(top.sel, [top.last]);
    await menuOpen(page, 'Edit'); await pick(page, 'Paste in Back');
    const bot = await C(page, E => { const its = E.items(); return { n: its.length, sel: E.selection(), first: its[0].id }; });
    assert.equal(bot.n, n0 + 2); assert.deepEqual(bot.sel, [bot.first]);
    await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await settle(page);
    assert.equal(await C(page, E => E.items().length), n0);
  });
  await step('1.6 Select ▸ All selects every part that can be; Select ▸ Deselect; Object ▸ Lock ▸ Selection and Unlock All', async () => {
    await closeM(page); await page.evaluate(() => window.CharmEditor.current.ai.act('deselect')); await settle(page);
    await menuOpen(page, 'Select'); await pick(page, 'All');
    const n = await C(page, E => E.selection().length); assert.ok(n > 10, 'selected ' + n);
    await menuOpen(page, 'Select'); await pick(page, 'Deselect');
    assert.equal(await C(page, E => E.selection().length), 0);
    await sel(page, ['heart-line']);
    await menuOpen(page, 'Object'); await subRows(page, 'Lock'); await pick(page, 'Selection');
    assert.equal(await C(page, E => E.items().find(i => i.id === 'heart-line').locked), true);
    await menuOpen(page, 'Object'); await pick(page, 'Unlock All');
    assert.ok(await C(page, E => !E.items().find(i => i.id === 'heart-line').locked));
  });
  await step('1.7 View: Outline, the zooms, Screen Mode ▸, Rulers ▸, Smart Guides ✓, Guides ▸, Show/Hide Grid; Rulers ▸ Hide Rulers hides them and the charm stays put on screen', async () => {
    await menuOpen(page, 'View');
    const r = await rows(page);
    for (const w of ['Outline [Ctrl+Y]', 'Zoom In [Ctrl++]', 'Zoom Out [Ctrl+-]', 'Fit Artboard in Window [Ctrl+0]', 'Fit All in Window [Alt+Ctrl+0]', 'Actual Size [Ctrl+1]', 'Screen Mode ▸', 'Rulers ▸', 'Smart Guides [Ctrl+U] ✓', 'Guides ▸', "Hide Grid [Ctrl+']", "Snap to Grid [Shift+Ctrl+']"]) assert.ok(r.includes(w), w + ' in ' + r.join(' | '));
    const k0 = await C(page, E => { const f = E.frame(); return E.clientOf(f.x0, f.y0); });
    await subRows(page, 'Rulers'); await pick(page, 'Hide Rulers');
    const hid = await page.evaluate(() => getComputedStyle(document.querySelector('.ce-rule--x')).visibility);
    const k1 = await C(page, E => { const f = E.frame(); return E.clientOf(f.x0, f.y0); });
    assert.equal(hid, 'hidden'); assert.ok(Math.abs(k1.x - k0.x) < 1 && Math.abs(k1.y - k0.y) < 1, JSON.stringify([k0, k1]));
    await menuOpen(page, 'View'); assert.deepEqual(await subRows(page, 'Rulers'), ['Show Rulers [Ctrl+R]', 'Reset Zero Point']); await pick(page, 'Show Rulers');
    assert.equal(await page.evaluate(() => getComputedStyle(document.querySelector('.ce-rule--x')).visibility), 'visible');
    await menuOpen(page, 'View'); await pick(page, 'Hide Grid');
    assert.equal(await page.evaluate(() => document.querySelector('.ce-grid').innerHTML), '');
    await menuOpen(page, 'View'); await pick(page, 'Show Grid');
  });
  await step('1.8 File: Close, Save, Send to Sheet… only when it can be done, Place… (the importers); Window: Toolbars/Control, Layers ✓ F7, alphabetical; Type: Font ▸ with the fonts, Size ▸, Font Style ▸, Change Case ▸', async () => {
    await menuOpen(page, 'File'); const f = await rows(page);
    assert.ok(f.includes('Save [Ctrl+S]') && f.includes('Place… [Shift+Ctrl+P]') && f.includes('Close'), f.join(' | '));
    await menuOpen(page, 'Window'); const w = await rows(page);
    assert.ok(w.includes('Layers [F7] ✓') && w.includes('Tools ✓') && w.includes('Control ✓'), w.join(' | '));
    await menuOpen(page, 'Type'); const t = await rows(page);
    for (const x of ['Font (off) ▸', 'Size (off) ▸', 'Font Style (off) ▸', 'Change Case (off) ▸', 'Glyphs… (off)']) assert.ok(t.includes(x), x + ' in ' + t.join(' | '));   // (no words selected: greyed, as Illustrator greys them)
    await closeM(page);
  });
  await step('1.9 the keys of a menu: F10 opens File with a row in focus, → goes to Edit and on to Object, its first row (Transform ▸) → opens the submenu with its first row in focus, ↓ walks it, Esc closes one level, Esc the menu (the focus back on its title)', async () => {
    await closeM(page); await sel(page, ['heart-fill']);
    await page.keyboard.press('F10'); await settle(page);
    const a = await page.evaluate(() => ({ open: document.querySelector('.ce-mb__t.is-open') && document.querySelector('.ce-mb__t.is-open').textContent, focus: !!document.activeElement.closest('.ce-aimenu') }));
    assert.deepEqual(a, { open: 'File', focus: true });
    await page.keyboard.press('ArrowRight'); await settle(page);
    assert.equal(await page.evaluate(() => document.querySelector('.ce-mb__t.is-open').textContent), 'Edit');
    await page.keyboard.press('ArrowRight'); await settle(page);
    assert.equal(await page.evaluate(() => document.activeElement.querySelector('.ce-aimi__l').textContent), 'Transform');
    await page.keyboard.press('ArrowRight'); await settle(page);
    const s1 = await page.evaluate(() => { const m = document.activeElement.closest('.ce-aimenu--sub'); return m && !m.hidden ? [m.getAttribute('aria-label'), document.activeElement.querySelector('.ce-aimi__l').textContent] : null; });
    assert.ok(s1 && s1[0] === 'Transform', JSON.stringify(s1));
    await page.keyboard.press('ArrowDown'); await settle(page);
    const s2 = await page.evaluate(() => document.activeElement.querySelector('.ce-aimi__l').textContent);
    assert.notEqual(s2, s1[1]);
    await page.keyboard.press('Escape'); await settle(page);
    assert.equal(await page.evaluate(() => document.activeElement.querySelector('.ce-aimi__l').textContent), 'Transform');
    await page.keyboard.press('Escape'); await settle(page);
    assert.deepEqual(await page.evaluate(() => [document.querySelectorAll('.ce-aimenu').length, document.activeElement.textContent]), [0, 'Object']);
    return s1[1] + ' → ' + s2;
  });

  console.log('2 · the Tools panel');
  await step('2.1 Illustrator\'s order and groups: Selection, Direct Selection, Marquee | Pen, Type, Line Segment, Rectangle, Pencil, Scissors | Free Distort, Blend | Hand, Zoom | Edit Toolbar ···', async () => {
    const r = await page.evaluate(() => [...document.querySelectorAll('.ce-tools > .ce-tool, .ce-tools > .ce-drop > .ce-tool, .ce-tools > .ce-tsep')].map(b => b.classList.contains('ce-tsep') ? '|' : b.dataset.tool || b.dataset.tip));
    assert.deepEqual(r, ['select', 'direct', 'marquee', '|', 'pen', 'text', 'line', 'rect', 'pencil', 'scissors', '|', 'cw-free', 'cw-blend', '|', 'hand', 'zoom', 'more']);
    const fl = await page.evaluate(() => ({ shapes: [...document.querySelectorAll('[data-drop="shapes"] .ce-menu [data-tool]')].map(b => b.dataset.tool), pen: [...document.querySelectorAll('[data-drop="x-Pen"] .ce-menu [data-tool]')].map(b => b.dataset.tool), corner: !!document.querySelector('[data-drop="shapes"] .ce-drop__b') }));
    assert.deepEqual(fl.shapes, ['rect', 'shape-rrect', 'ellipse', 'polygon', 'shape-star', 'shape-heart', 'shape-ring', 'shape-bail']);
    assert.deepEqual(fl.pen, ['pen', 'anchorAdd', 'anchorDel', 'anchorConvert']);
  });
  await step('2.2 the rich tool tip: "Selection Tool (V)" and a line on what it does; the Rectangle\'s says (M), the Pen\'s (P)', async () => {
    await closeM(page);
    const tipOf = async sel => { await page.mouse.move(5, 400); await settle(page); await page.hover(sel); await page.waitForTimeout(420); return page.evaluate(() => { const t = document.querySelector('.ce-tip.is-in'); return t ? t.querySelector('b').textContent : null; }); };
    assert.equal(await tipOf('.ce-tools [data-tool="select"]'), 'Selection Tool(V)');
    assert.equal(await tipOf('[data-drop="shapes"] .ce-drop__b'), 'Rectangle Tool(M)');
    assert.equal(await tipOf('[data-drop="x-Pen"] .ce-drop__b'), 'Pen Tool(P)');
    await page.mouse.move(700, 500);
  });
  await step('2.3 a flyout opens beside its tool (the corner), the tool picked takes the slot; the » folds the panel into two columns and back, remembered', async () => {
    const b = await page.evaluate(() => { const r = document.querySelector('[data-drop="shapes"] .ce-drop__b').getBoundingClientRect(); return { x: r.right - 4, y: r.bottom - 4 }; });
    await page.mouse.click(b.x, b.y); await settle(page);
    await page.click('[data-drop="shapes"] .ce-menu [data-tool="ellipse"]'); await settle(page);
    assert.equal(await C(page, E => E.tool()), 'ellipse');
    assert.equal(await page.evaluate(() => document.querySelector('[data-drop="shapes"] .ce-drop__b').dataset.tool), 'ellipse');
    await page.keyboard.press('v');
    const w1 = await page.evaluate(() => document.querySelector('.ce-rail').getBoundingClientRect().width);
    await page.click('.ce-rail__cols'); await settle(page); await settle(page);
    const r2 = await page.evaluate(() => ({ two: document.querySelector('.ce-rail').classList.contains('is-2col'), w: document.querySelector('.ce-rail').getBoundingClientRect().width, cols: getComputedStyle(document.querySelector('.ce-tools')).gridTemplateColumns.split(' ').length, kept: JSON.parse(localStorage.getItem('ce.ai')).cols }));
    assert.ok(r2.two && r2.cols === 2 && r2.w > w1 + 10 && r2.kept === 2, JSON.stringify(r2));
    await shot(page, 'ai-tools-2col-1440.png');
    await page.click('.ce-rail__cols'); await settle(page);
    assert.equal(await page.evaluate(() => document.querySelector('.ce-rail').classList.contains('is-2col')), false);
  });

  console.log('3 · the Control bar');
  await step('3.1 nothing selected: "No Selection", X/Y/W/H empty; a path: "Path", Fill / Stroke (the laser role), the reference point, X Y W H in mm', async () => {
    await closeM(page); await page.evaluate(() => window.CharmEditor.current.ai.act('deselect')); await settle(page);
    assert.equal(await page.evaluate(() => document.querySelector('[data-selkind]').textContent), 'No Selection');
    await clickPart(page, 'heart-fill');
    const r = await page.evaluate(() => ({ k: document.querySelector('[data-selkind]').textContent, role: !document.querySelector('[data-drop="role"]').hidden, fill: document.querySelector('[data-ctlsw="fill"]').classList.contains('is-none'), stroke: document.querySelector('[data-ctlsw="stroke"]').classList.contains('is-none'), x: document.querySelector('[data-f="x"]').value, ref: !!document.querySelector('.ce-refpt [data-refpt="0,0"].is-on'), text: document.querySelector('[data-drop="fstyle"]').hidden }));
    assert.deepEqual(r, Object.assign({}, r, { k: 'Path', role: true, fill: false, stroke: true, ref: true, text: true }));
    assert.ok(+r.x > 0, r.x);
  });
  await step('3.2 the reference point: the centre picked, X and Y read the middle of the box; W typed keeps the middle where it was', async () => {
    const b = await C(page, E => E.box('heart-fill'));
    await page.click('.ce-refpt [data-refpt="0.5,0.5"]'); await settle(page);
    const x = +(await page.locator('[data-f="x"]').inputValue()), zero = await C(page, E => E.zero());
    assert.ok(Math.abs(x - ((b[0] + b[2]) / 2 - zero[0]) * 25.4 / 72) < 0.006, x);
    await page.locator('[data-f="w"]').click(); await page.keyboard.press('Control+a'); await page.keyboard.type('4'); await page.keyboard.press('Enter'); await settle(page);
    const b2 = await C(page, E => E.box('heart-fill'));
    assert.ok(Math.abs((b2[0] + b2[2]) / 2 - (b[0] + b[2]) / 2) < 0.01 && Math.abs((b2[1] + b2[3]) / 2 - (b[1] + b[3]) / 2) < 0.01, 'centre kept');
    assert.ok(Math.abs((b2[2] - b2[0]) * 25.4 / 72 - 4) < 0.01);
    await page.keyboard.press('Control+z'); await page.click('.ce-refpt [data-refpt="0,0"]'); await settle(page);
  });
  await step('3.3 words selected: "Type", Character: the font, its style (Regular ▾ → Bold), the size with its presets, Paragraph alignment', async () => {
    await page.click('.ce-head [data-face="back"]'); await settle(page);
    await clickPart(page, 'b-words');
    const r = await page.evaluate(() => ({ k: document.querySelector('[data-selkind]').textContent, shown: ['[data-drop="font"]', '[data-drop="fstyle"]', '[data-f="pt"]', '[data-drop="tsize"]', '.ce-ctl__para'].map(s => { const e = document.querySelector('.ce-props ' + s); return !!e && !e.closest('[hidden]') && e.getBoundingClientRect().width > 0; }), style: document.querySelector('[data-fstylename]').textContent }));
    assert.equal(r.k, 'Type'); assert.deepEqual(r.shown, [true, true, true, true, true]); assert.equal(r.style, 'Regular');
    await page.click('[data-drop="fstyle"] .ce-drop__b'); await settle(page); await page.click('[data-fstyle="bold"]'); await settle(page);
    assert.equal(await C(page, E => !!E.items().find(i => i.id === 'b-words').bold), true);
    assert.equal(await page.evaluate(() => document.querySelector('[data-fstylename]').textContent), 'Bold');
    await page.click('[data-drop="tsize"] .ce-drop__b'); await settle(page); await page.click('[data-tsize="12"]'); await settle(page);
    assert.equal(await C(page, E => E.items().find(i => i.id === 'b-words').sizePt), 12);
    await page.click('.ce-ctl__para [data-talign="right"]'); await settle(page);
    assert.equal(await C(page, E => E.items().find(i => i.id === 'b-words').align), 'right');
    await shot(page, 'ai-control-type-1440.png');
    await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await settle(page);
  });

  console.log('4 · document tabs, status bar, Fill / Stroke');
  await step('4.1 the document tabs: the piece as the document, "Front" and "Back", the active one with its zoom and view ("@ 600% (Preview)"); the status bar\'s artboard navigator goes between them', async () => {
    const t = await page.evaluate(() => [...document.querySelectorAll('.ce-doctabs [data-face]')].map(b => b.textContent));
    assert.ok(/: Front$/.test(t[0]) && /: Back @ \d+% \(Preview\)$/.test(t[1]), t.join(' | '));
    await page.click('.ce-abnav [data-ai-face="first"]'); await settle(page);
    assert.equal(await C(page, E => E.face()), 'front');
    assert.equal(await page.evaluate(() => document.querySelector('[data-ai-facename]').textContent), '1 Front');
    await page.click('.ce-abnav [data-ai-face="next"]'); await settle(page);
    assert.equal(await C(page, E => E.face()), 'back');
    await page.click('.ce-doctabs [data-face="front"]'); await settle(page);
  });
  await step('4.2 the status bar: the zoom (its presets), the tool in hand by name', async () => {
    await page.keyboard.press('h'); await settle(page);
    assert.equal(await page.evaluate(() => document.querySelector('[data-ai-toolname]').textContent), 'Hand');
    await page.keyboard.press('v');
    assert.equal(await page.evaluate(() => document.querySelector('[data-ai-toolname]').textContent), 'Selection');
  });
  await step('4.3 Fill / Stroke in the Tools panel follow the selection (a line engraving: no fill, a red stroke); Swap Fill and Stroke makes it a hatch fill (one undo step)', async () => {
    await sel(page, ['heart-line']);
    const a = await page.evaluate(() => ({ f: document.querySelector('[data-paintsw="fill"]').classList.contains('is-none'), s: document.querySelector('[data-paintsw="stroke"]').classList.contains('is-none') }));
    assert.deepEqual(a, { f: true, s: false });
    await page.click('.ce-paint__mini--swap'); await settle(page);
    assert.equal(await C(page, E => E.items().find(i => i.id === 'heart-line').role), 'hatch');
    await page.keyboard.press('Control+z'); await settle(page);
    assert.equal(await C(page, E => E.items().find(i => i.id === 'heart-line').role), 'line');
  });
  await step('4.4 Window ▸ Toolbars and Control hide the Tools panel and the Control bar; the menu brings them back', async () => {
    await menuOpen(page, 'Window'); await pick(page, 'Control');
    assert.equal(await page.evaluate(() => getComputedStyle(document.querySelector('.ce-props')).display), 'none');
    await menuOpen(page, 'Window'); await pick(page, 'Control');
    assert.notEqual(await page.evaluate(() => getComputedStyle(document.querySelector('.ce-props')).display), 'none');
  });

  console.log('5 · the theme');
  await step('5.1 Medium Dark by default (Illustrator\'s), Source Sans 3 at 12 px, 24 px menu rows, Spectrum blue under the pointer', async () => {
    const r = await page.evaluate(() => { const root = document.querySelector('.ce-root'), cs = getComputedStyle(root); return { ui: root.dataset.ui, panel: cs.getPropertyValue('--ai-panel').trim(), font: cs.fontFamily, fs: cs.fontSize }; });
    assert.equal(r.ui, 'mdark'); assert.equal(r.panel, '#535353'); assert.ok(/Source Sans 3/.test(r.font), r.font); assert.equal(r.fs, '12px');
    await menuOpen(page, 'View');
    const row = await page.evaluate(() => { const b = document.querySelector('.ce-aimenu .ce-aimi'); return b.getBoundingClientRect().height; });
    assert.ok(row >= 23.5 && row <= 25, 'row ' + row);
    await closeM(page);
  });
  await step('5.2 Edit ▸ Preferences ▸ User Interface…: the four brightnesses change the workspace at once (Light: light panels, dark text), Canvas Color White, kept for this person (localStorage ce.ai); Cancel puts it back', async () => {
    await menuOpen(page, 'Edit'); await subRows(page, 'Preferences'); await pick(page, 'User Interface…');
    assert.ok(await page.evaluate(() => !!document.querySelector('.ce-aidlg [data-pf-ui="light"]')));
    const look = () => page.evaluate(() => { const cs = getComputedStyle(document.querySelector('.ce-props')); return { ui: document.querySelector('.ce-root').dataset.ui, bg: cs.backgroundColor, color: getComputedStyle(document.querySelector('[data-selkind]')).color }; });
    const seen = {};
    for (const u of ['dark', 'mlight', 'light', 'mdark']) { await page.click(`.ce-aidlg [data-pf-ui="${u}"]`); await settle(page); seen[u] = await look(); }
    assert.equal(seen.dark.bg, 'rgb(50, 50, 50)'); assert.equal(seen.mlight.bg, 'rgb(184, 184, 184)'); assert.equal(seen.light.bg, 'rgb(241, 241, 241)'); assert.equal(seen.light.color, 'rgb(34, 34, 34)');
    await page.click('.ce-aidlg [data-pf-ui="light"]'); await page.check('.ce-aidlg [data-pf-canvas][value="white"]'); await settle(page);
    if (SHOTS) await shot(page, 'ai-prefs-light-1440.png');
    await page.click('.ce-aidlg [data-dlg="ok"]'); await settle(page);
    const kept = await page.evaluate(() => ({ p: JSON.parse(localStorage.getItem('ce.ai')), pb: getComputedStyle(document.querySelector('.ce-stage')).backgroundColor }));
    assert.equal(kept.p.ui, 'light'); assert.equal(kept.p.canvas, 'white'); assert.equal(kept.pb, 'rgb(255, 255, 255)');
    await menuOpen(page, 'Edit'); await subRows(page, 'Preferences'); await pick(page, 'User Interface…');
    await page.click('.ce-aidlg [data-pf-ui="dark"]'); await settle(page);
    assert.equal((await look()).ui, 'dark');
    await page.click('.ce-aidlg [data-dlg="cancel"]'); await settle(page);
    assert.equal((await look()).ui, 'light', 'Cancel: back to what it was');
  });
  await step('5.3 the next workspace opens in the person\'s brightness', async () => {
    await page.evaluate(() => window.CharmEditor.current.end(false)); await settle(page);
    await openIt(page);
    assert.equal(await page.evaluate(() => document.querySelector('.ce-root').dataset.ui), 'light');
    await shot(page, 'ai-light-1440.png');
    await page.evaluate(() => { localStorage.removeItem('ce.ai'); window.CharmEditor.current.end(false); }); await settle(page);
  });
  await step('5.4 no page error', async () => { assert.deepEqual(page.__errs, []); });
  await page.context().close();

  console.log('6 · sizes');
  for (const [w, h] of [[1440, 900], [1024, 768]]) {
    const pg = await newPage(w, h);
    await openIt(pg);
    await step(`6.${w} at ${w} × ${h}: the regions do not overlap, nothing of the Application bar or the status bar is cut off or off screen; the Tools panel one column`, async () => {
      const r = await pg.evaluate(() => {
        const R = s => { const e = document.querySelector(s); const b = e.getBoundingClientRect(); return [b.left, b.top, b.right, b.bottom]; };
        const regs = ['.ce-appbar', '.ce-props', '.ce-doctabs', '.ce-rail', '.ce-wrap', '.ce-foot'].map(R), bad = [];
        for (let i = 0; i < regs.length; i++) for (let j = i + 1; j < regs.length; j++) { const a = regs[i], b = regs[j]; if (Math.min(a[2], b[2]) - Math.max(a[0], b[0]) > 0.75 && Math.min(a[3], b[3]) - Math.max(a[1], b[1]) > 0.75) bad.push(i + '/' + j); }
        const vis = el => { const b = el.getBoundingClientRect(); return b.width > 0.5 && !el.closest('[hidden]') && getComputedStyle(el).display !== 'none'; };
        const bar = [...document.querySelectorAll('.ce-appbar > *, .ce-foot > *')].filter(vis);
        for (const e of bar) { const b = e.getBoundingClientRect(); if (b.left < -0.5 || b.right > innerWidth + 0.5) bad.push('off ' + e.className); }
        return { bad, menus: document.querySelectorAll('.ce-mb__t').length && getComputedStyle(document.querySelector('.ce-mb__t')).display !== 'none', two: getComputedStyle(document.querySelector('.ce-tools')).gridTemplateColumns.split(' ').length };
      });
      assert.deepEqual(r.bad, []); assert.ok(r.menus); assert.equal(r.two, 1);
    });
    await shot(pg, `ai-workspace-${w}.png`);
    await clickPart(pg, 'heart-fill');
    await shot(pg, `ai-selection-${w}.png`);
    await pg.click('.ce-menubar .ce-mb__t:text-is("Object")'); await settle(pg);
    await pg.hover('.ce-aimenu .ce-aimi--sub:has(.ce-aimi__l:text-is("Path"))'); await settle(pg);
    await shot(pg, `ai-menu-object-path-${w}.png`);
    await pg.keyboard.press('Escape');
    await step(`6.${w} no page error`, async () => { assert.deepEqual(pg.__errs, []); });
    await pg.context().close();
  }
  {
    const pg = await newPage(390, 844);
    await openIt(pg);
    await step('6.390 a phone: the menus behind one ☰ that opens them all as submenus; nothing in the Application bar off screen', async () => {
      const r = await pg.evaluate(() => ({ all: getComputedStyle(document.querySelector('.ce-mb__all')).display !== 'none', t: getComputedStyle(document.querySelector('.ce-mb__t')).display }));
      assert.ok(r.all && r.t === 'none', JSON.stringify(r));
      await pg.click('.ce-mb__all'); await settle(pg);
      const rr = await pg.evaluate(() => [...document.querySelectorAll('.ce-aimenu:not([hidden]) > .ce-aisub > .ce-aimi .ce-aimi__l')].map(x => x.textContent));
      assert.deepEqual(rr, ['File', 'Edit', 'Object', 'Type', 'Select', 'Effect', 'View', 'Window', 'Help']);
      await shot(pg, 'ai-phone-menu.png');
      await pg.keyboard.press('Escape');
    });
    await pg.context().close();
  }

  await browser.close();
  console.log(`\n${results.length} passed, ${failures.length} failed`);
  if (failures.length) { console.log(failures.map(f => '  - ' + f).join('\n')); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
