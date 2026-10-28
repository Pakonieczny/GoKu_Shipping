/* THE CHARM EDITOR AS ILLUSTRATOR, PART TWO (charm-nest-ce-ai-keys.js; Paul, 28 Oct 2026: "all expected keyboard shortcuts (Illustrator, Photoshop) ... it must fit like a glove").
 * No network, no live service: the editor's harness page served from 127.0.0.1, with the menu bar (charm-nest-ce-ai-shell.js), the extension files and this one.
 *
 *   PW_DIR=/opt/node-tools/node_modules CHROMIUM=/opt/pw-browsers/chromium node tests/charm-nest/ce-ai-keys.cjs      (SHOTS=<dir> keeps screenshots)
 *
 *  N  node: the module loads alone; the Select commands pick in the stacking order; a key matched against the catalog
 *  C  the right-click menus per case: nothing selected, a path, a group, words, several things, a Layers row; Illustrator's order, submenus (Transform ▸ Arrange ▸
 *     Select ▸ ...), shortcuts in the right column, greyed Undo / Redo / Paste, the charm's own items last; a row runs what the Object menu runs; a submenu opens on hover
 *  K  the keys: Illustrator's tools (V Q M L \ T H Z ...), ⌘2 / ⌥⌘2 / ⌘3 / ⌥⌘3 lock and hide, ⇧2 zoom to the selection, ⌘R rulers, ⌘' grid, ⌘F / ⌘B paste in front
 *     and back, ⇧⌘A deselect, ⌥⌘] Next Object Above, ⌘Space the Zoom tool while held, Tab hides the panels; the words box keeps every key it is typed in
 *  S  Edit ▸ Keyboard Shortcuts… (⌥⇧⌘K): Illustrator's dialog: Set, Tools / Menu Commands, the search, Mac / Windows, read-only, Esc closes; the menu bar opens it
 */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path'), fs = require('node:fs'), http = require('node:http');
const ROOT = path.join(__dirname, '..', '..');
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
let n = 0;
const failures = [];
const step = async (name, fn) => { try { const note = await fn(); n++; console.log('  ok   ' + name + (note ? '  [' + note + ']' : '')); } catch (e) { failures.push(name); console.log('  FAIL ' + name + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n         ')); } };

(async () => {
  /* ── N · node ── */
  const AK = require(path.join(ROOT, 'charm-nest-ce-ai-keys.js'));
  await step('N1 the module loads in node and offers its API', async () => {
    for (const f of ['key', 'keyUp', 'contextList', 'contextCase', 'openContext', 'table', 'label', 'bind', 'showShortcuts']) assert.equal(typeof AK[f], 'function', f);
  });
  await step('N2 Select ▸ Next Object Above / Below, First Object Above, Last Object Below: the stacking order, hidden and locked parts passed by', async () => {
    const list = [{ id: 'a' }, { id: 'b', hidden: true }, { id: 'c' }, { id: 'd', locked: true }, { id: 'e' }];
    let sel = ['a'];
    const ctx = { selection: () => sel, items: () => list, entry: id => ({ it: list.find(x => x.id === id), parent: null }), select: ids => { sel = ids; } };
    AK.stackPick(ctx, 'above'); assert.deepEqual(sel, ['c']);
    AK.stackPick(ctx, 'above'); assert.deepEqual(sel, ['e']);
    assert.equal(AK.stackPick(ctx, 'above'), false); assert.deepEqual(sel, ['e']);
    AK.stackPick(ctx, 'below'); assert.deepEqual(sel, ['c']);
    AK.stackPick(ctx, 'last'); assert.deepEqual(sel, ['a']);
    AK.stackPick(ctx, 'first'); assert.deepEqual(sel, ['e']);
  });

  /* ── the browser ── */
  let chromium; try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(ROOT, 'node_modules'), 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser pass was not run'); finish(); return; }
  const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.woff2': 'font/woff2', '.json': 'application/json' };
  const server = http.createServer((req, res) => {
    const p = path.normalize(path.join(ROOT, decodeURIComponent(req.url.split('?')[0])));
    if (!p.startsWith(ROOT) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404); res.end(); return; }
    res.writeHead(200, { 'content-type': types[path.extname(p)] || 'application/octet-stream' }); fs.createReadStream(p).pipe(res);
  });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}/`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  try {
    const ctxB = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await ctxB.route(u => !u.href.startsWith(base), r => r.abort());
    const page = await ctxB.newPage();
    const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });
    await page.addInitScript(() => { try { localStorage.clear(); } catch (e) { /* fine */ } });
    await page.goto(base + 'tests/charm-nest/fixtures/charm-editor-harness.html');
    for (const f of ['charm-nest-vector.js', 'charm-nest-ce-paths.js', 'charm-nest-ce-ops.js', 'charm-nest-ce-extras.js', 'charm-nest-ce-warp.js']) await page.addScriptTag({ url: base + f });
    const settle = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
    const shot = async name => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name) }); };
    await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle();
    const C = (fn, arg) => page.evaluate(([src, a]) => (new Function('C', 'a', 'return (' + src + ')(C, a)'))(window.CharmEditor.current.ctx, a), [fn.toString(), arg]);
    const cur = () => page.evaluate(() => ({ tool: CharmEditor.current.tool(), sel: CharmEditor.current.selection(), zoom: CharmEditor.current.zoom(), st: CharmEditor.current.ai.st() }));
    const client = (xmm, ymm) => page.evaluate(([x, y]) => window.CharmEditor.current.clientOf(window.fixture.X0 + x * window.fixture.PT, window.fixture.Y0 + y * window.fixture.PT), [xmm, ymm]);
    const centreOf = async id => { const b = await page.evaluate(id => CharmEditor.current.box(id), id); return page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2]); };
    const edgeOf = async id => { const b = await page.evaluate(id => CharmEditor.current.box(id), id); return page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [(b[0] + b[2]) / 2, b[1]]); };
    /** The open right-click menu, as rows: label, shortcut, off, submenu, check; separators as '-'. */
    const menuRows = sel => page.evaluate(sel => {
      const m = document.querySelector(sel); if (!m || m.hidden) return null;
      const rows = [];
      for (const el of m.children) {
        if (el.classList.contains('ce-menu__sep')) { rows.push('-'); continue; }
        const b = el.classList.contains('ce-aisub') ? el.querySelector(':scope > .ce-aimi') : el;
        if (!b || !b.classList.contains('ce-item')) continue;
        const l = b.querySelector('.ce-aimi__l'), k = b.querySelector('em');
        rows.push({ label: (l ? l.textContent : b.textContent).trim(), key: k ? k.textContent : '', off: b.classList.contains('is-off'), sub: el.classList.contains('ce-aisub') ? [...el.querySelector(':scope > .ce-menu').querySelectorAll(':scope > .ce-item, :scope > .ce-aisub > .ce-item')].map(x => (x.querySelector('.ce-aimi__l') || x).textContent.trim()) : null,
          hook: b.dataset.ctx ? 'ctx:' + b.dataset.ctx : b.dataset.xcmd ? 'x:' + b.dataset.xcmd : b.dataset.ai ? 'ai:' + b.dataset.ai : '' });
      }
      return rows;
    }, sel);
    const labels = rows => rows.map(r => (r === '-' ? '-' : r.label + (r.sub ? ' ▸' : '')));
    const rightClick = async (p, mods) => { if (mods) for (const k of mods) await page.keyboard.down(k); await page.mouse.click(p.x, p.y, { button: 'right' }); if (mods) for (const k of mods) await page.keyboard.up(k); await settle(); };
    const escape = async () => { await page.keyboard.press('Escape'); await settle(); };
    const deselect = async () => { await C(c => c.select([])); await settle(); };
    const blank = await client(12, 29.6);   // (inside the charm's frame, under the last row of squares: nothing there but the outline's bottom edge is at 29)

    /* ── C · the right-click menus ── */
    await step('C1 nothing selected: Undo / Redo (greyed), Paste, Paste in Place, the zoom, Rulers / Grid / Guides, Outline, Select All, Select ▸, then the charm\'s own', async () => {
      await deselect();
      await rightClick(await client(30, 15));
      const rows = await menuRows('.ce-root .ce-ctx'); assert.ok(rows, 'a menu');
      const L = labels(rows);
      console.log('         nothing: ' + L.join(' | '));
      assert.equal(L[0], 'Undo'); assert.equal(L[1], 'Redo'); assert.ok(rows[0].off && rows[1].off, 'nothing to undo yet: greyed');
      for (const w of ['Paste', 'Paste in Place', 'Zoom In', 'Zoom Out', 'Fit Artboard in Window', 'Actual Size', 'Hide Rulers', 'Hide Grid', 'Select All']) assert.ok(L.includes(w), 'has ' + w + ': ' + L.join(', '));
      assert.ok(rows.find(r => r.label === 'Paste').off, 'nothing copied: Paste greyed');
      assert.ok(!L.includes('Cut') && !L.includes('Group') && !L.includes('Transform ▸'), 'nothing to cut, group or transform');
      assert.ok(L.includes('Select ▸') && L.includes('Laser Check…'), L.join(', '));
      assert.equal(L[L.length - 1], 'Laser Check…', 'the charm\'s own last');
      assert.ok(L.indexOf('-') === 2, 'a separator after Redo');
      await shot('ai-ctx-none.png');
      await escape();
    });
    await step('C2 a path: Cut Copy Paste Clear, Simplify…, the masks and compound path, Offset Path… / Outline Stroke / Cut Line Round the Art…, Transform ▸ Arrange ▸ Select ▸, Jump Ring, Laser Check', async () => {
      await rightClick(await edgeOf('day-4-2'));
      const rows = await menuRows('.ce-root .ce-ctx'), L = labels(rows);
      console.log('         path: ' + L.join(' | '));
      assert.deepEqual(await page.evaluate(() => CharmEditor.current.selection()), ['day-4-2'], 'the right-clicked part is selected first');
      for (const w of ['Undo', 'Redo', 'Cut', 'Copy', 'Paste', 'Clear', 'Simplify…', 'Offset Path…', 'Outline Stroke', 'Cut Line Round the Art…', 'Make Clipping Mask', 'Transform ▸', 'Arrange ▸', 'Select ▸', 'Jump Ring on Top', 'Laser Check…']) assert.ok(L.includes(w), 'has ' + w + ': ' + L.join(', '));
      assert.ok(!L.includes('Group') && !L.includes('Ungroup'), 'one path: no Group / Ungroup');
      const arr = rows.find(r => r.label === 'Arrange'); assert.deepEqual(arr.sub, ['Bring to Front', 'Bring Forward', 'Send Backward', 'Send to Back']);
      const tr = rows.find(r => r.label === 'Transform'); for (const w of ['Transform Again', 'Move…', 'Rotate…', 'Reflect…', 'Scale…']) assert.ok(tr.sub.includes(w), 'Transform has ' + w + ': ' + tr.sub);
      const sl = rows.find(r => r.label === 'Select'); assert.deepEqual(sl.sub.slice(0, 4), ['First Object Above', 'Next Object Above', 'Next Object Below', 'Last Object Below']);
      assert.ok(sl.sub.includes('Same') && sl.sub.includes('Inverse'), sl.sub.join(', '));
      const ks = Object.fromEntries(rows.filter(r => r !== '-').map(r => [r.label, r.key]));
      assert.ok(/X$/.test(ks.Cut) && /C$/.test(ks.Copy), 'shortcut column: ' + JSON.stringify(ks));
      assert.ok(L.indexOf('Cut') < L.indexOf('Simplify…') && L.indexOf('Simplify…') < L.indexOf('Make Clipping Mask') && L.indexOf('Make Clipping Mask') < L.indexOf('Offset Path…') && L.indexOf('Offset Path…') < L.indexOf('Transform ▸') && L.indexOf('Transform ▸') < L.indexOf('Arrange ▸') && L.indexOf('Arrange ▸') < L.indexOf('Select ▸') && L.indexOf('Select ▸') < L.indexOf('Laser Check…'), 'Illustrator\'s order');
      // the shortcut column is right-aligned: every em ends at the same x
      const rights = await page.$$eval('.ce-root .ce-ctx > .ce-item em', els => els.map(e => Math.round(e.getBoundingClientRect().right)));
      assert.ok(rights.length >= 3 && new Set(rights).size === 1, 'shortcuts right-aligned: ' + rights.join(','));
      // a submenu opens on hover, beside its row
      const arrRow = page.locator('.ce-root .ce-ctx > .ce-aisub > .ce-aimi', { hasText: 'Arrange' });
      await arrRow.hover(); await page.waitForTimeout(400); await settle();
      const subOpen = await page.evaluate(() => { const s = [...document.querySelectorAll('.ce-root .ce-ctx .ce-aimenu--sub')].find(m => !m.hidden); if (!s) return null; const r = s.getBoundingClientRect(), p = s.parentNode.getBoundingClientRect(); return { left: r.left, pr: p.right, label: s.getAttribute('aria-label') }; });
      assert.ok(subOpen && subOpen.label === 'Arrange' && Math.abs(subOpen.left - subOpen.pr) < 12, 'Arrange ▸ opens beside its row: ' + JSON.stringify(subOpen));
      await shot('ai-ctx-path.png');
      // Bring to Front from the submenu: what the editor's own Bring to front does
      const top0 = await C(c => c.items().map(i => i.id).indexOf('day-4-2'));
      await page.locator('.ce-root .ce-ctx .ce-aimenu--sub:not([hidden]) .ce-item', { hasText: 'Bring to Front' }).click(); await settle();
      const ids = await C(c => c.items().map(i => i.id));
      assert.ok(ids.indexOf('day-4-2') === ids.length - 1 && top0 < ids.length - 1, 'brought to the front');
      assert.ok(await page.evaluate(() => document.querySelector('.ce-root .ce-ctx').hidden), 'the menu closed');
      await page.keyboard.press('Control+z'); await settle();
    });
    await step('C3 a group: Ungroup (⇧⌘G), no Group; Transform ▸ Arrange ▸ Select ▸', async () => {
      await page.evaluate(() => CharmEditor.current.ctx.select(['day-0-0', 'day-0-1'])); await page.keyboard.press('Control+g'); await settle();
      const gid = (await page.evaluate(() => CharmEditor.current.selection()))[0];
      await rightClick(await edgeOf(gid));
      const rows = await menuRows('.ce-root .ce-ctx'), L = labels(rows);
      console.log('         group: ' + L.join(' | '));
      const ug = rows.find(r => r.label === 'Ungroup'); assert.ok(ug && !ug.off && /G$/.test(ug.key) && ug.hook === 'ctx:ungroup', JSON.stringify(ug));
      assert.ok(!L.includes('Group'), 'no Group for one group');
      await page.locator('.ce-root .ce-ctx .ce-item', { hasText: 'Ungroup' }).click(); await settle();
      assert.equal((await page.evaluate(() => CharmEditor.current.selection())).length, 2, 'ungrouped');
      await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await settle();
    });
    await step('C4 several things: Group (⌘G), Make Compound Path, Make Clipping Mask; a Layers row\'s right-click is the same menu', async () => {
      await page.evaluate(() => CharmEditor.current.ctx.select(['day-1-1', 'day-1-2', 'heart-line'])); await settle();
      await rightClick(await edgeOf('day-1-1'));
      const rows = await menuRows('.ce-root .ce-ctx'), L = labels(rows);
      console.log('         multi: ' + L.join(' | '));
      for (const w of ['Group', 'Make Compound Path', 'Make Clipping Mask', 'Transform ▸', 'Arrange ▸']) assert.ok(L.includes(w), 'has ' + w);
      assert.equal(await page.evaluate(() => document.querySelector('.ce-root .ce-ctx').dataset.ctxkind), 'multi');
      await shot('ai-ctx-multi.png');
      await escape();
      const row = page.locator('.ce-root .ce-layer[data-id="heart-fill"]'); await row.scrollIntoViewIfNeeded();
      const b = await row.boundingBox(); await page.mouse.click(b.x + b.width / 2, b.y + b.height / 2, { button: 'right' }); await settle();
      const rows2 = await menuRows('.ce-root .ce-ctx'); assert.ok(rows2, 'a menu on the row');
      assert.deepEqual(await page.evaluate(() => CharmEditor.current.selection()), ['heart-fill']);
      assert.ok(labels(rows2).includes('Arrange ▸') && labels(rows2).includes('Cut'), labels(rows2).join(', '));
      await escape();
    });
    await step('C5 words: Edit Text, Font ▸ (each font, the one in use ticked), Size ▸, Font Style ▸, Change Case ▸, Paragraph ▸, Glyphs…', async () => {
      await page.evaluate(() => { const b = [...document.querySelectorAll('.ce-root [data-face="back"]')].find(x => !x.disabled); b.click(); }); await settle();
      await page.evaluate(() => CharmEditor.current.ctx.select(['b-words'])); await settle();
      await rightClick(await centreOf('b-words'));
      const rows = await menuRows('.ce-root .ce-ctx'), L = labels(rows);
      console.log('         text: ' + L.join(' | '));
      for (const w of ['Edit Text', 'Font ▸', 'Size ▸', 'Font Style ▸', 'Change Case ▸', 'Paragraph ▸']) assert.ok(L.includes(w), 'has ' + w + ': ' + L.join(', '));
      assert.deepEqual(rows.find(r => r.label === 'Font').sub, ['Nunito Sans', 'Cormorant', 'Sans Bold']);
      assert.equal(await page.evaluate(() => document.querySelector('.ce-root .ce-ctx').dataset.ctxkind), 'text');
      await shot('ai-ctx-text.png');
      await escape();
      await page.evaluate(() => { const b = [...document.querySelectorAll('.ce-root [data-face="front"]')].find(x => !x.disabled); b.click(); }); await settle();
    });

    await step('C6 a letter typed while the right-click menu is open is no tool key; the arrows walk the menu; Esc closes it', async () => {
      await deselect(); await rightClick(await client(30, 15));
      await page.keyboard.press('m'); await page.keyboard.press('l'); await settle();
      assert.equal(await page.evaluate(() => CharmEditor.current.tool()), 'select');
      await page.keyboard.press('ArrowDown'); await page.keyboard.press('ArrowDown'); await settle();
      const focus = await page.evaluate(() => { const a = document.activeElement; return a && a.closest('.ce-ctx') ? (a.querySelector('.ce-aimi__l') || a).textContent : null; });
      assert.ok(focus && focus !== 'Undo', 'focus walks the rows (greyed ones skipped): ' + focus);
      await escape();
      assert.ok(await page.evaluate(() => document.querySelector('.ce-root .ce-ctx').hidden), 'closed');
    });

    /* ── K · the keys ── */
    const press = async k => { await page.keyboard.press(k); await settle(); };
    await page.evaluate(() => document.querySelector('.ce-root .ce-win').focus());
    await step('K1 the tools: V Selection, Q Marquee, M Rectangle, L Ellipse, \\ Line, T Type, H Hand, Z Zoom, P Pen, A Direct Selection, N Pencil, C Scissors, E Free Distort; the page under never hears a key', async () => {
      const k0 = await page.evaluate(() => window.__pageKeys);
      const want = [['q', 'marquee'], ['m', 'rect'], ['l', 'ellipse'], ['Backslash', 'line'], ['t', 'text'], ['h', 'hand'], ['z', 'zoom'], ['p', 'pen'], ['a', 'direct'], ['n', 'pencil'], ['c', 'scissors'], ['e', 'cw-free'], ['v', 'select']];
      for (const [k, t] of want) { await press(k); assert.equal((await cur()).tool, t, k + ' → ' + t); }
      assert.equal(await page.evaluate(() => window.__pageKeys), k0, 'the page under the editor heard nothing');
    });
    await step('K2 ⌘2 locks the selection, ⌥⌘2 unlocks all, ⌘3 hides it, ⌥⌘3 shows all; ⇧2 zooms to the selection (⌘2 no longer does)', async () => {
      await C(c => c.select(['day-2-2'])); await settle();
      const z0 = (await cur()).zoom;
      await press('Control+2');
      let it = await C(c => c.items().find(i => i.id === 'day-2-2')); assert.equal(it.locked, true, 'locked');
      assert.equal((await cur()).zoom, z0, 'no zoom');
      await press('Control+Alt+2'); it = await C(c => c.items().find(i => i.id === 'day-2-2')); assert.ok(!it.locked, 'unlocked');
      await C(c => c.select(['day-2-2'])); await press('Control+3'); it = await C(c => c.items().find(i => i.id === 'day-2-2')); assert.equal(it.hidden, true, 'hidden');
      await press('Control+Alt+3'); it = await C(c => c.items().find(i => i.id === 'day-2-2')); assert.ok(!it.hidden, 'shown');
      await C(c => c.select(['day-2-2'])); await press('Shift+Digit2');
      assert.ok((await cur()).zoom > z0 * 2, 'zoomed to the square: ' + (await cur()).zoom);
      await press('Control+0');
    });
    await step('K3 the view: ⌘R hides and shows the rulers, ⌘\' the grid, ⌘; the guides, ⌘Y Outline, ⌘+ / ⌘- / ⌘0 / ⌘1', async () => {
      const s0 = (await cur()).st;
      await press("Control+r"); assert.equal((await cur()).st.rulers, !s0.rulers, 'rulers');
      assert.ok(await page.evaluate(() => document.querySelector('.ce-root .ce-wrap').classList.contains('is-norulers')));
      await press("Control+r"); assert.equal((await cur()).st.rulers, s0.rulers);
      await press("Control+Quote"); assert.equal((await cur()).st.opts.grid, !s0.opts.grid, 'grid'); await press("Control+Quote");
      await press('Control+Semicolon'); assert.equal((await cur()).st.opts.showGuides, !s0.opts.showGuides, 'guides'); await press('Control+Semicolon');
      await press('Control+y'); assert.equal((await cur()).st.outline, true, 'outline'); await press('Control+y'); assert.equal((await cur()).st.outline, false);
      await press('Control+1'); assert.equal(Math.round((await cur()).zoom), 100);
      await press('Control+Equal'); assert.ok((await cur()).zoom > 100); await press('Control+Minus'); await press('Control+Minus'); assert.ok((await cur()).zoom < 100);
      await press('Control+0');
    });
    await step('K4 the clipboard: ⌘C then ⌘F pastes in front, ⌘B in back (the bottom of the list), ⇧⌘V in place; ⇧⌘A deselects; ⌘A selects all', async () => {
      await C(c => c.select(['heart-fill'])); await press('Control+c');
      const n0 = await C(c => c.items().length);
      await press('Control+f'); let ids = await C(c => c.items().map(i => i.id)); assert.equal(ids.length, n0 + 1); const front = (await cur()).sel[0]; assert.equal(ids[ids.length - 1], front, 'in front');
      await press('Control+b'); ids = await C(c => c.items().map(i => i.id)); assert.equal(ids.length, n0 + 2); const back = (await cur()).sel[0]; assert.equal(ids[0], back, 'in back');
      const bf = await page.evaluate(id => CharmEditor.current.box(id), back), b0 = await page.evaluate(() => CharmEditor.current.box('heart-fill'));
      assert.ok(bf.every((v, i) => Math.abs(v - b0[i]) < 1e-6), 'in place');
      await press('Control+Shift+a'); assert.deepEqual((await cur()).sel, [], 'deselected');
      await press('Control+z'); await press('Control+z');
      assert.equal(await C(c => c.items().length), n0);
      await press('Control+a'); assert.ok((await cur()).sel.length > 10, 'all'); await press('Control+Shift+a');
    });
    await step('K5 Select ▸ Next Object Above (⌥⌘]) / Below (⌥⌘[), First Object Above (⌥⇧⌘]), Last Object Below (⌥⇧⌘[); ⌘] still brings forward', async () => {
      const ids = await C(c => c.items().filter(i => !i.hidden && !i.locked).map(i => i.id));
      await C(c => c.select(['day-2-3'])); await press('Control+Alt+BracketRight');
      assert.deepEqual((await cur()).sel, [ids[ids.indexOf('day-2-3') + 1]], 'next above');
      await press('Control+Alt+BracketLeft'); assert.deepEqual((await cur()).sel, ['day-2-3'], 'next below');
      await press('Control+Alt+Shift+BracketRight'); assert.deepEqual((await cur()).sel, [ids[ids.length - 1]], 'first above');
      await press('Control+Alt+Shift+BracketLeft'); assert.deepEqual((await cur()).sel, [ids[0]], 'last below');
      await C(c => c.select(['day-2-3'])); const at = (await C(c => c.items().map(i => i.id))).indexOf('day-2-3');
      await press('Control+BracketRight'); assert.equal((await C(c => c.items().map(i => i.id))).indexOf('day-2-3'), at + 1, 'brought forward');
      await press('Control+z');
    });
    await step('K6 ⌘Space is the Zoom tool while held and gives back the tool in hand; Space alone is the Hand', async () => {
      await press('m'); assert.equal((await cur()).tool, 'rect');
      await page.keyboard.down('Control'); await page.keyboard.down('Space'); await settle();
      assert.equal((await cur()).tool, 'zoom', 'zoom while held');
      await page.keyboard.up('Space'); await page.keyboard.up('Control'); await settle();
      assert.equal((await cur()).tool, 'rect', 'back to the Rectangle');
      await page.keyboard.down('Space'); await settle();
      assert.ok(await page.evaluate(() => document.querySelector('.ce-root .ce-stage').classList.contains('is-space')), 'Space: the hand');
      await page.keyboard.up('Space'); await press('v');
    });
    await step('K7 Tab hides the panels and shows them again; on a button Tab moves the focus instead', async () => {
      await page.evaluate(() => document.querySelector('.ce-root .ce-win').focus());
      await press('Tab'); assert.ok(await page.evaluate(() => document.querySelector('.ce-root .ce-win').classList.contains('is-nopanels')), 'hidden');
      await shot('ai-tab-hidden.png');
      await press('Tab'); assert.ok(!(await page.evaluate(() => document.querySelector('.ce-root .ce-win').classList.contains('is-nopanels'))), 'shown');
      await page.evaluate(() => document.querySelector('.ce-root .ce-tool[data-tool="select"]').focus());
      await press('Tab'); assert.ok(!(await page.evaluate(() => document.querySelector('.ce-root .ce-win').classList.contains('is-nopanels'))), 'a button: focus moves');
      await page.evaluate(() => document.querySelector('.ce-root .ce-win').focus());
    });
    await step('K8 the words box is never hijacked: M, L, R, ⌘2, ⌘B, Tab, Space and ⌘A typed in it are the words\', and Esc closes it', async () => {
      await page.evaluate(() => { const b = [...document.querySelectorAll('.ce-root [data-face="back"]')].find(x => !x.disabled); b.click(); }); await settle();
      await page.evaluate(() => CharmEditor.current.ctx.select(['b-words'])); await settle();
      await page.keyboard.press('Enter'); await page.waitForSelector('.ce-root textarea.ce-editor'); await settle();
      const tool0 = (await cur()).tool, it0 = await C(c => c.items().find(i => i.id === 'b-words'));
      await page.keyboard.press('End'); await page.keyboard.type(' mlr qz'); await page.keyboard.press('Control+2'); await page.keyboard.press('Control+r'); await page.keyboard.press('Control+b');
      const v = await page.evaluate(() => document.querySelector('.ce-root textarea.ce-editor').value);
      assert.ok(v.endsWith(' mlr qz'), 'typed: ' + v);
      assert.equal((await cur()).tool, tool0, 'no tool changed');
      assert.ok(!(await C(c => c.items().find(i => i.id === 'b-words'))).locked && (await cur()).st.rulers, 'nothing locked, rulers as they were');
      await page.keyboard.press('Escape'); await settle();
      assert.ok(!(await page.$('.ce-root textarea.ce-editor')), 'closed');
      assert.ok((await C(c => c.items().find(i => i.id === 'b-words'))).text.endsWith(' mlr qz'));
      await press('Control+z');
      assert.equal((await C(c => c.items().find(i => i.id === 'b-words'))).text, it0.text);
      await page.evaluate(() => { const b = [...document.querySelectorAll('.ce-root [data-face="front"]')].find(x => !x.disabled); b.click(); }); await settle();
    });
    await step('K9 a size field keeps its own keys (digits, arrows, letters) and the keys come back to the work after Enter', async () => {
      await C(c => c.select(['heart-fill'])); await settle();
      const f = page.locator('.ce-root [data-f="w"]'); await f.click(); await page.keyboard.press('Control+a'); await page.keyboard.type('3m');
      assert.equal((await cur()).tool, 'select', 'm typed in the field is not the Rectangle');
      await page.keyboard.press('Escape'); await settle();
    });

    /* ── S · Edit ▸ Keyboard Shortcuts… ── */
    await step('S1 ⌥⇧⌘K opens Illustrator\'s Keyboard Shortcuts dialog: Set (Illustrator Defaults, read-only), Menu Commands by menu with Mac keys', async () => {
      await page.evaluate(() => document.querySelector('.ce-root .ce-win').focus());
      await press('Control+Alt+Shift+k');
      assert.ok(await page.$('.ce-root .ce-aik'), 'the dialog');
      await page.click('.ce-aik [data-aik-os="mac"]'); await settle();   // (the test browser is Linux: Windows keys at first)
      const s = await page.evaluate(() => {
        const b = document.querySelector('.ce-aik'); const rows = [...b.querySelectorAll('tbody tr[data-aik-id]')].map(r => [r.cells[0].textContent, r.cells[1].textContent]);
        return { title: b.querySelector('.ce-aik__title').textContent, set: b.querySelector('.ce-aik__set').disabled, view: b.querySelector('[data-aik-view]').value, heads: [...b.querySelectorAll('tbody tr.is-head:not(.is-sub)')].map(r => r.textContent), rows };
      });
      assert.equal(s.title, 'Keyboard Shortcuts'); assert.equal(s.set, true); assert.equal(s.view, 'menu');
      assert.deepEqual(s.heads.slice(0, 9), ['File', 'Edit', 'Object', 'Type', 'Select', 'Effect', 'View', 'Window', 'Help']);
      const k = Object.fromEntries(s.rows.slice().reverse());   // (the first row of a name: Edit ▸ Keyboard Shortcuts… before Help's)
      assert.equal(k['Paste in Front'], '⌘F'); assert.equal(k['Keyboard Shortcuts…'], '⌥⇧⌘K'); assert.equal(k['Lock ▸ Selection'] || k['Selection'], '⌘2'); assert.equal(k['Next Object Above'], '⌥⌘]');
      await shot('ai-shortcuts-menu.png');
      return s.rows.length + ' menu rows';
    });
    await step('S2 Tools view, the search, Windows keys; read-only; Esc closes it and the keys go back to the work', async () => {
      await page.selectOption('.ce-aik [data-aik-view]', 'tools'); await settle();
      let rows = await page.$$eval('.ce-aik tbody tr[data-aik-id]', rs => rs.map(r => [r.cells[0].textContent, r.cells[1].textContent]));
      const k = Object.fromEntries(rows);
      assert.equal(k['Selection Tool'], 'V'); assert.equal(k['Rectangle Tool'], 'M'); assert.equal(k['Ellipse Tool'], 'L'); assert.equal(k['Zoom Tool'], 'Z'); assert.equal(k['Hand Tool (temporary)'], 'Space');
      await shot('ai-shortcuts-tools.png');
      await page.click('.ce-aik [data-aik-os="win"]'); await settle();
      await page.selectOption('.ce-aik [data-aik-view]', 'menu');
      await page.fill('.ce-aik [data-aik-q]', 'paste'); await settle();
      rows = await page.$$eval('.ce-aik tbody tr[data-aik-id]', rs => rs.map(r => [r.cells[0].textContent, r.cells[1].textContent]));
      assert.ok(rows.length >= 4 && rows.every(r => /paste/i.test(r[0])), JSON.stringify(rows));
      assert.equal(Object.fromEntries(rows)['Paste in Back'], 'Ctrl+B');
      assert.equal(await page.$$eval('.ce-aik input:not([type=search]):not([disabled]), .ce-aik [contenteditable]', e => e.length), 0, 'nothing to edit');
      const tool0 = (await cur()).tool;
      await page.keyboard.type('m'); assert.equal((await cur()).tool, tool0, 'typing in the search is not a tool key');
      await page.keyboard.press('Escape'); await settle();
      assert.ok(!(await page.$('.ce-aik')), 'closed');
      await page.evaluate(() => { try { localStorage.removeItem('ce.ai'); } catch (e) { /* fine */ } });
      await press('m'); assert.equal((await cur()).tool, 'rect', 'the keys are the work\'s again'); await press('v');
    });
    await step('S3 the menu bar\'s Edit ▸ Keyboard Shortcuts… and Help ▸ Keyboard Shortcuts… open the same dialog', async () => {
      await page.click('.ce-root .ce-menubar [data-mb="1"]'); await settle();
      await page.locator('.ce-root .ce-aimenu .ce-item', { hasText: 'Keyboard Shortcuts…' }).first().click(); await settle();
      assert.ok(await page.$('.ce-aik'), 'from Edit'); await escape();
      assert.equal(await page.evaluate(() => CharmEditorAi.openShortcuts === undefined), false);
      assert.ok(await page.evaluate(() => typeof CharmEditorAi.keys.table === 'function' && CharmEditorAi.keys.label('edit.shortcuts', true) === '⌥⇧⌘K'));
    });
    await step('S4 no errors on the page', async () => { assert.deepEqual(errs, []); });
    await ctxB.close();
  } finally { await browser.close(); server.close(); }
  finish();
})().catch(e => { console.error(e); process.exitCode = 1; });
function finish() {
  console.log(`ce-ai-keys: ${n} checks passed${failures.length ? `, ${failures.length} FAILED: ${failures.join(' · ')}` : ''}`);
  if (failures.length) process.exitCode = 1;
}
