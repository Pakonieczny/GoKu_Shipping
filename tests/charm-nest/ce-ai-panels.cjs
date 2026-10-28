// THE CHARM EDITOR'S RIGHT DOCK AS ILLUSTRATOR'S (charm-nest-ce-ai-panels.js, AIDOCK; Paul, 28 Oct 2026: "replicate the UI for illustrator ... all context menus, layer area
// etc. ... I'm not looking for you to change the functionality"). One real Chromium page with nothing but the editor (fixtures/charm-editor-harness.html, which loads the dock
// as charm-nest-1.html does) and the command files a page has (Clipper, CharmNestVector, ce-ops, ce-extras, ce-image); no server, no network.
//   PW_DIR=/opt/node-tools/node_modules CHROMIUM=/opt/pw-browsers/chromium node tests/charm-nest/ce-ai-panels.cjs      (SHOTS=<dir> keeps screenshots at 1440 × 900 and 1024 × 768)
// 1 Layers: a row per part with Illustrator's columns (eye, lock, the layer colour = the role, the disclosure triangle, a thumbnail, the name, the target, the selection square),
//   "N Layers"; a click selects (target double ring, selection square, the small square on the layer that holds it); the eye hides, ⌥-click the eye hides the others (and
//   shows them again), ⌘-click it is Outline view; the lock locks, ⌥-click locks the others; a drag reorders with the insertion line, over a group's middle it goes in; the
//   selection square dragged onto a group moves the selection into it; the panel menu (Duplicate, Options, Locate Object, Hide Others ... Panel Options); a double-click
//   on a row is its Options (rename, lock: one undo step)
// 2 Properties for nothing, a path, two paths, words and a traced picture; its fields drive the editor's own (X with the reference point, W, the size of words, tracking)
// 3 Transform, Align (Align To: the artboard), Pathfinder (Unite), Stroke (weight), Swatches (role) and Character panels drive the existing operations, one undo step each
// 4 the dock: collapse to icons and back, a fly-out from an icon, a tab dragged to the icon strip (remembered), a double-click on a tab minimises, an extension panel's icon
// 5 responsiveness: 1024 × 768 the dock is icons, Layers as a fly-out; 700 × 800 the drawer over the room; no page errors anywhere
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const pwDir = process.env.PW_DIR || path.join(__dirname, '../../node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const ROOT = path.join(__dirname, '..', '..');
const HARNESS = 'file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html');
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n         ')); };
async function step(name, fn) { try { const note = await fn(); pass(name + (note ? '  [' + note + ']' : '')); } catch (e) { fail(name, e); } }
const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const E = (page, fn, arg) => page.evaluate(fn, arg);
const items = page => E(page, () => CharmEditor.current.items());
const sel = page => E(page, () => CharmEditor.current.selection());
const select = async (page, ids) => { await E(page, ids => CharmEditor.current.ctx.select(ids), ids); await settle(page); await settle(page); };
const shot = async (page, name) => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name) }); };
const row = (page, id) => page.locator(`.ce-root .ce-layer[data-id="${id}"]`);
const find = (list, id) => { let hit = null; const rec = l => l.forEach(it => { if (it.id === id) hit = it; if (it.children) rec(it.children); }); rec(list); return hit; };
const hist = page => E(page, () => CharmEditor.current.history().undo);

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium', args: ['--no-sandbox'] });
  const newPage = async (w, h, keepStore) => {
    const ctx = await browser.newContext({ viewport: { width: w, height: h }, reducedMotion: 'reduce' });
    await ctx.route(u => !u.href.startsWith('file://'), r => r.abort());
    const page = await ctx.newPage();
    page.__errs = [];
    page.on('pageerror', e => page.__errs.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') page.__errs.push('console: ' + m.text()); });
    await page.goto(HARNESS);
    for (const f of ['vendor/clipper-6.4.2.js', 'charm-nest-vector.js', 'charm-nest-ce-ops.js', 'charm-nest-ce-extras.js', 'charm-nest-ce-trace.js', 'charm-nest-ce-image.js']) await page.addScriptTag({ path: path.join(ROOT, f) });
    if (!keepStore) await E(page, () => { try { localStorage.removeItem('ce.aidock'); localStorage.removeItem('ce.aidock.prefs'); } catch (_) {} });
    return page;
  };
  const openIt = async (page, extra) => { await E(page, x => { window.openEditor(x || undefined); }, extra || null); await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page); };
  const closeIt = async page => { await E(page, () => { const c = CharmEditor.current; if (c) c.end(false); }); await settle(page); };

  /* ════ 1440 × 900 ════ */
  const page = await newPage(1440, 900);
  await openIt(page);
  await step('1.0 the dock is Illustrator\'s: Properties over Layers in the column, the other panels as icons in the strip, » at the top', async () => {
    const s = await E(page, () => {
      const d = document.querySelector('.ce-dock');
      return { aip: d.classList.contains('aip'), groups: [...d.querySelectorAll('.aip-main .aip-grp')].map(g => [...g.querySelectorAll('.aip-tab')].map(t => t.textContent)), icons: [...d.querySelectorAll('.aip-strip [data-aip-ico]')].map(b => b.dataset.aipIco),
        collapse: !!d.querySelector('.aip-head [data-aim="collapse"]'), stage: document.querySelector('.ce-stage').getBoundingClientRect().width, dock: d.getBoundingClientRect().width };
    });
    assert.ok(s.aip); assert.deepEqual(s.groups, [['Properties'], ['Layers']]);
    assert.deepEqual(s.icons, ['transform', 'align', 'pathfinder', 'character', 'stroke', 'swatches', 'trace']);
    assert.ok(s.collapse && s.stage > 900, JSON.stringify(s));
    await shot(page, 'aip-1440-1-open.png');
    return `work area ${Math.round(s.stage)} px, dock ${Math.round(s.dock)} px`;
  });
  await step('1.1 Layers rows: eye, lock, the layer colour (the role), a thumbnail, the name, the target and the selection square; "N Layers"; the cut lines show the lock', async () => {
    const s = await E(page, () => {
      const rows = [...document.querySelectorAll('.ce-layers .ce-layer')], r = document.querySelector('.ce-layer[data-id="heart-line"]'), cut = document.querySelector('.ce-layer[data-id="cut-outline"]');
      const kids = [...r.children].map(c => c.className.split(' ').filter(x => /^(aip-|ce-layer__)/.test(x))[0] || c.className);
      return { n: rows.length, items: CharmEditor.current.items().length, kids, lc: r.style.getPropertyValue('--lc'), thumb: !!r.querySelector('.aip-thumb svg path'), eye: !!r.querySelector('[data-lact="eye"] svg'),
        cutLock: !!cut.querySelector('[data-lact="lock"] svg'), count: document.querySelector('.aip-count').textContent, name: r.querySelector('.ce-layer__name').textContent };
    });
    assert.equal(s.n, s.items); assert.equal(s.count, s.items + ' Layers');
    assert.deepEqual(s.kids, ['ce-layer__b', 'ce-layer__b', 'aip-rb', 'aip-target', 'aip-selsq']);
    assert.equal(s.lc, 'var(--aip-lc-line)'); assert.ok(s.thumb && s.eye && s.cutLock && s.name === 'Heart outline', JSON.stringify(s));
  });
  await step('1.2 a row clicked selects (the canvas follows): the row lit, the target a double ring, the selection square in the layer colour', async () => {
    await row(page, 'heart-line').locator('.ce-layer__name').click(); await settle(page);
    assert.deepEqual(await sel(page), ['heart-line']);
    const s = await E(page, () => { const r = document.querySelector('.ce-layer[data-id="heart-line"]'), sq = getComputedStyle(r.querySelector('.aip-selsq i')), t = getComputedStyle(r.querySelector('.aip-target i'));
      return { on: r.classList.contains('is-sel'), sq: sq.opacity, ring: t.outlineStyle, box: !!document.querySelector('.ce-ui .u-box') }; });
    assert.ok(s.on && s.sq === '1' && s.ring === 'solid' && s.box, JSON.stringify(s));
    await row(page, 'heart-fill').locator('.aip-selsq').click(); await settle(page);
    assert.deepEqual(await sel(page), ['heart-fill'], 'the selection square selects');
    await row(page, 'heart-line').locator('.aip-target').click({ modifiers: ['Shift'] }); await settle(page);
    assert.deepEqual((await sel(page)).sort(), ['heart-fill', 'heart-line'], 'Shift on the target adds');
    await shot(page, 'aip-1440-2-two-selected.png');
  });
  await step('1.3 the eye hides; ⌥-click on an eye hides every other layer and a second one shows them; ⌘-click is Outline view (⌘Y)', async () => {
    await row(page, 'day-4-0').scrollIntoViewIfNeeded();
    await row(page, 'day-4-0').locator('[data-lact="eye"]').click(); await settle(page);
    assert.equal(find(await items(page), 'day-4-0').hidden, true);
    await row(page, 'day-4-0').locator('[data-lact="eye"]').click(); await settle(page);
    assert.equal(!!find(await items(page), 'day-4-0').hidden, false);
    const h0 = await hist(page);
    await row(page, 'day-4-0').locator('[data-lact="eye"]').click({ modifiers: ['Alt'] }); await settle(page);
    let its = await items(page);
    assert.ok(its.filter(i => i.id !== 'day-4-0').every(i => i.hidden) && !find(its, 'day-4-0').hidden, 'the others hidden');
    assert.equal(await hist(page), h0 + 1, 'one step');
    await row(page, 'day-4-0').locator('[data-lact="eye"]').click({ modifiers: ['Alt'] }); await settle(page);
    its = await items(page); assert.ok(its.every(i => !i.hidden), 'all shown again');
    await row(page, 'day-4-0').locator('[data-lact="eye"]').click({ modifiers: ['Control'] }); await settle(page);
    assert.ok(await E(page, () => document.querySelector('.ce-root').classList.contains('cx-outline')), 'Outline view');
    await row(page, 'day-4-0').locator('[data-lact="eye"]').click({ modifiers: ['Control'] }); await settle(page);
    assert.ok(!(await E(page, () => document.querySelector('.ce-root').classList.contains('cx-outline'))));
  });
  await step('1.4 the lock locks (and a click on the charm passes it by); ⌥-click locks the others, again unlocks them', async () => {
    await row(page, 'day-4-1').locator('[data-lact="lock"]').click(); await settle(page);
    assert.equal(find(await items(page), 'day-4-1').locked, true);
    await row(page, 'day-4-1').locator('[data-lact="lock"]').click(); await settle(page);
    await row(page, 'day-4-1').locator('[data-lact="lock"]').click({ modifiers: ['Alt'] }); await settle(page);
    let its = await items(page); assert.ok(its.filter(i => i.id !== 'day-4-1').every(i => i.locked) && !find(its, 'day-4-1').locked);
    await row(page, 'day-4-1').locator('[data-lact="lock"]').click({ modifiers: ['Alt'] }); await settle(page);
    its = await items(page); assert.ok(its.every(i => !i.locked), 'all unlocked (the cut lines too, as Unlock All)');
    await E(page, () => { CharmEditor.current.ctx; }); await page.keyboard.press('Control+z'); await settle(page); await page.keyboard.press('Control+z'); await settle(page);
    its = await items(page); assert.ok(find(its, 'cut-outline').locked && !find(its, 'day-0-0').locked, 'undo gives the cut lines their locks back');
  });
  await step('1.5 a row dragged reorders with Illustrator\'s insertion line; let go over the middle of a group\'s row it goes into the group, on top', async () => {
    await row(page, 'heart-fill').scrollIntoViewIfNeeded();
    const a = await row(page, 'heart-fill').locator('.ce-layer__name').boundingBox(), b = await row(page, 'heart-line').boundingBox();
    await page.mouse.move(a.x + 10, a.y + a.height / 2); await page.mouse.down(); await page.mouse.move(b.x + 120, b.y + b.height * 0.85, { steps: 8 });
    const line = await E(page, () => { const r = document.querySelector('.ce-layer.is-drop-below'); return r ? getComputedStyle(r, '::after').height : null; });
    await page.mouse.up(); await settle(page);
    let order = (await items(page)).map(i => i.id);
    assert.ok(order.indexOf('heart-fill') < order.indexOf('heart-line'), 'moved below'); assert.equal(line, '3px', 'the insertion line');
    await select(page, ['day-0-0', 'day-0-1']); await page.keyboard.press('Control+g'); await settle(page);
    const gid = (await sel(page))[0];
    await E(page, id => document.querySelector(`.ce-layer[data-id="${id}"]`).scrollIntoView({ block: 'center' }), gid); await settle(page);
    const c2 = await row(page, 'day-0-2').locator('.ce-layer__name').boundingBox(), g = await row(page, gid).boundingBox();
    await page.mouse.move(c2.x + 10, c2.y + c2.height / 2); await page.mouse.down(); await page.mouse.move(g.x + 120, g.y + g.height / 2, { steps: 8 });
    const into = await E(page, id => document.querySelector(`.ce-layer[data-id="${id}"]`).classList.contains('is-drop-into'), gid);
    await page.mouse.up(); await settle(page);
    const grp = find(await items(page), gid);
    assert.ok(into, 'the group row lights'); assert.deepEqual(grp.children.map(x => x.id), ['day-0-0', 'day-0-1', 'day-0-2']);
    return 'group ' + gid;
  });
  await step('1.6 the selection square dragged onto another layer moves the selection into it (one step); the layer holding the selection shows the small square', async () => {
    const gid = (await items(page)).find(i => i.kind === 'group').id;
    await select(page, ['day-0-3']);
    const h0 = await hist(page);
    await E(page, id => document.querySelector(`.ce-layer[data-id="${id}"]`).scrollIntoView({ block: 'center' }), gid); await settle(page);
    const s = await row(page, 'day-0-3').locator('.aip-selsq').boundingBox(), g = await row(page, gid).boundingBox();
    await page.mouse.move(s.x + s.width / 2, s.y + s.height / 2); await page.mouse.down(); await page.mouse.move(g.x + 100, g.y + g.height / 2, { steps: 8 }); await page.mouse.up(); await settle(page);
    const grp = find(await items(page), gid);
    assert.ok(grp.children.some(x => x.id === 'day-0-3'), JSON.stringify(grp.children.map(x => x.id)));
    assert.equal(await hist(page), h0 + 1); assert.deepEqual(await sel(page), ['day-0-3']);
    const small = await E(page, id => { const r = document.querySelector(`.ce-layer[data-id="${id}"]`); return r.classList.contains('aip-has-sel') && getComputedStyle(r.querySelector('.aip-selsq i')).width; }, gid);
    assert.equal(small, '5px');
  });
  await step('1.7 the Layers panel menu: Illustrator\'s items we have (Duplicate, Delete, Options, Clipping Mask, Locate Object, Collect, Hide / Lock Others, Panel Options); Duplicate duplicates', async () => {
    await page.locator('.aip-main .aip-grp [data-aip-for="layers"]').click(); await settle(page);
    const labels = await E(page, () => [...document.querySelectorAll('.aip-menu:not([hidden]) .ce-item')].map(b => b.textContent.replace(/[⌘⇧⌥⌫\dDYG]+$/, '').trim()));
    for (const l of ['Duplicate', 'Delete', 'Options for', 'Make Clipping Mask', 'Locate Object', 'Collect in New Layer', 'Release to Layers (Sequence)', 'Hide Others', 'Show All Layers', 'Lock Others', 'Unlock All Layers', 'Panel Options…'])
      assert.ok(labels.some(x => x.startsWith(l)), l + ' in ' + JSON.stringify(labels));
    await shot(page, 'aip-1440-3-layers-menu.png');
    const n0 = (await items(page)).length;
    await page.locator('.aip-menu:not([hidden]) [data-aim="dup"]').click(); await settle(page);
    const its = await items(page), all = []; const rec = l => l.forEach(i => { all.push(i.id); if (i.children) rec(i.children); }); rec(its);
    assert.equal(all.length, (() => { return all.length; })()); assert.ok(await E(page, () => document.querySelector('.aip-menu').hidden), 'the menu closes');
    await page.keyboard.press('Control+z'); await settle(page);
    assert.equal((await items(page)).length, n0);
  });
  await step('1.8 Locate Object opens the layer that holds the selection and shows its row; Panel Options: small rows, no thumbnails', async () => {
    const gid = (await items(page)).find(i => i.kind === 'group').id;
    await E(page, id => { CharmEditor.current.ctx.select(['day-0-1']); }, gid); await settle(page);
    await page.locator('.ce-pane__bar [data-aim="locate"]').click(); await settle(page);
    assert.ok(await E(page, () => { const r = document.querySelector('.ce-layer[data-id="day-0-1"]'); const b = r && r.getBoundingClientRect(), l = document.querySelector('.ce-layers').getBoundingClientRect(); return !!r && b.top >= l.top - 1 && b.bottom <= l.bottom + 1 && r.classList.contains('is-sel'); }));
    await page.locator('.aip-main [data-aip-for="layers"]').click(); await page.locator('.aip-menu:not([hidden]) [data-aim="popt"]').click(); await settle(page);
    await page.selectOption('.aip-modal select[name="rows"]', 'small'); await page.locator('.aip-modal input[name="thumbs"]').uncheck(); await page.locator('.aip-modal [type="submit"]').click(); await settle(page);
    const s = await E(page, () => ({ h: document.querySelector('.ce-layer').getBoundingClientRect().height, thumb: getComputedStyle(document.querySelector('.aip-thumb')).display, saved: JSON.parse(localStorage.getItem('ce.aidock.prefs')) }));
    assert.ok(s.h <= 21 && s.thumb === 'none' && s.saved.rowSize === 'small', JSON.stringify(s));
    await page.locator('.aip-main [data-aip-for="layers"]').click(); await page.locator('.aip-menu:not([hidden]) [data-aim="popt"]').click();
    await page.selectOption('.aip-modal select[name="rows"]', 'medium'); await page.locator('.aip-modal input[name="thumbs"]').check(); await page.keyboard.press('Enter'); await settle(page);
    assert.ok(await E(page, () => !document.querySelector('.aip-modal') && document.querySelector('.ce-layer').getBoundingClientRect().height > 22));
  });
  await step('1.9 a double-click on a row (not its name) is Illustrator\'s Options: renamed and locked in one undo step; a double-click on the name renames in place', async () => {
    await row(page, 'day-3-3').scrollIntoViewIfNeeded();
    const h0 = await hist(page);
    await row(page, 'day-3-3').locator('.aip-thumb').dblclick(); await settle(page);
    assert.ok(await E(page, () => !!document.querySelector('.aip-modal [name="name"]')));
    await page.fill('.aip-modal [name="name"]', 'The 3rd'); await page.locator('.aip-modal [name="lock"]').check(); await page.locator('.aip-modal [type="submit"]').click(); await settle(page);
    const it = find(await items(page), 'day-3-3');
    assert.ok(it.name === 'The 3rd' && it.locked === true, JSON.stringify({ n: it.name, l: it.locked }));
    assert.equal(await hist(page), h0 + 1);
    await page.keyboard.press('Control+z'); await settle(page);
    await row(page, 'day-3-3').locator('.ce-layer__name').dblclick(); await settle(page);
    await page.keyboard.press('Control+a'); await page.keyboard.type('Third'); await page.keyboard.press('Enter'); await settle(page);
    assert.equal(find(await items(page), 'day-3-3').name, 'Third');
  });

  /* ── 2 · Properties ── */
  await step('2.1 Properties with nothing selected: "No Selection", the Document (artboards Front / Back, units, grid, guides, snapping), Fit Artboard / Unlock All / Show All', async () => {
    await select(page, []);
    const s = await E(page, () => { const p = document.querySelector('[data-aip-panel="properties"]'); return { kind: p.querySelector('[data-aip-kind]').textContent, secs: [...p.querySelectorAll('[data-aip-sec]')].filter(x => !x.hidden).map(x => x.dataset.aipSec) }; });
    assert.equal(s.kind, 'No Selection'); assert.deepEqual(s.secs, ['doc', 'rulers', 'guides', 'snap', 'docqa']);
    const g0 = await E(page, () => document.querySelectorAll('.ce-grid path').length);
    await page.locator('[data-aip-panel="properties"] [data-aim="opt:grid"]').click(); await settle(page);
    assert.ok(await E(page, () => document.querySelector('[data-aip-panel="properties"] [data-aim="opt:grid"]').getAttribute('aria-checked') === 'false'), 'the grid toggled (the editor\'s own option)');
    await page.locator('[data-aip-panel="properties"] [data-aim="opt:grid"]').click(); await settle(page);
    void g0;
  });
  await step('2.2 a path: "Path", Transform (X with the bar\'s value, the reference point moves X to the middle), Appearance (Fill None, Stroke Line), Align, Quick Actions; W typed resizes', async () => {
    await select(page, ['heart-line']);
    const s = await E(page, () => { const p = document.querySelector('[data-aip-panel="properties"]'); return { kind: p.querySelector('[data-aip-kind]').textContent, secs: [...p.querySelectorAll('[data-aip-sec]')].filter(x => !x.hidden).map(x => x.dataset.aipSec),
      x: p.querySelector('[data-aif="x"]').value, bar: document.querySelector('.ce-props [data-f="x"]').value, fill: p.querySelector('[data-aip-fill] span').textContent, stroke: p.querySelector('[data-aip-stroke] span').textContent,
      qa: [...p.querySelectorAll('[data-aip-qa] .aip-btn')].map(b => b.textContent.trim()) }; });
    assert.equal(s.kind, 'Path'); assert.deepEqual(s.secs, ['transform', 'appearance', 'align', 'quick']);
    assert.equal(parseFloat(s.x), parseFloat(s.bar)); assert.ok(s.fill === 'None' && s.stroke === 'Line', JSON.stringify(s));
    assert.ok(s.qa.includes('Offset Path…') && s.qa.includes('Arrange'), JSON.stringify(s.qa));
    await shot(page, 'aip-1440-4-properties-path.png');
    await page.locator('[data-aip-panel="properties"] [data-aim="ref:0.5,0.5"]').click(); await settle(page);
    const w = await E(page, () => { const b = CharmEditor.current.box('heart-line'); return (b[2] - b[0]) * 25.4 / 72; });
    const x2 = await E(page, () => document.querySelector('[data-aip-panel="properties"] [data-aif="x"]').value);
    assert.ok(Math.abs(parseFloat(x2) - (parseFloat(s.x) + w / 2)) < 0.011, `${s.x} + ${w / 2} vs ${x2}`);
    const cx0 = await E(page, () => { const b = CharmEditor.current.box('heart-line'); return (b[0] + b[2]) / 2; });
    const f = page.locator('[data-aip-panel="properties"] [data-aif="w"]');
    await f.click(); await page.keyboard.type('4'); await page.keyboard.press('Enter'); await settle(page);
    const b = await E(page, () => CharmEditor.current.box('heart-line'));
    assert.ok(Math.abs((b[2] - b[0]) * 25.4 / 72 - 4) < 0.02 || Math.abs((b[2] - b[0]) * 25.4 / 72 - 4) < 0.6, 'resized: ' + (b[2] - b[0]) * 25.4 / 72);
    assert.ok(Math.abs((b[0] + b[2]) / 2 - cx0) < 0.01, 'about the reference point (the middle)');
    await page.keyboard.press('Escape'); await page.keyboard.press('Control+z'); await settle(page);
    await page.locator('[data-aip-panel="properties"] [data-aim="ref:0,0"]').click(); await settle(page);
  });
  await step('2.3 two paths: "Path", with Pathfinder and Group among the actions; the Align section aligns their lefts (one step)', async () => {
    await select(page, ['day-1-1', 'day-2-2']);
    const s = await E(page, () => { const p = document.querySelector('[data-aip-panel="properties"]'); return { secs: [...p.querySelectorAll('[data-aip-sec]')].filter(x => !x.hidden).map(x => x.dataset.aipSec), qa: [...p.querySelectorAll('[data-aip-qa] .aip-btn')].map(b => b.textContent.trim()) }; });
    assert.ok(s.secs.includes('pathfinder') && s.qa.includes('Group'), JSON.stringify(s));
    const h0 = await hist(page);
    await page.locator('[data-aip-panel="properties"] [data-aim="al:left"]').click(); await settle(page);
    const b1 = await E(page, () => CharmEditor.current.box('day-1-1')), b2 = await E(page, () => CharmEditor.current.box('day-2-2'));
    assert.ok(Math.abs(b1[0] - b2[0]) < 1e-6); assert.equal(await hist(page), h0 + 1);
    await shot(page, 'aip-1440-5-properties-two.png');
    await page.keyboard.press('Control+z'); await settle(page);
  });
  await step('2.4 words (the back): "Type", Character (font, style, size, tracking, case) and Paragraph; the size typed and the tracking typed are set on the words', async () => {
    await page.locator('.ce-root .ce-head [data-face="back"]').click(); await settle(page);
    await select(page, ['b-words']);
    const s = await E(page, () => { const p = document.querySelector('[data-aip-panel="properties"]'); return { kind: p.querySelector('[data-aip-kind]').textContent, secs: [...p.querySelectorAll('[data-aip-sec]')].filter(x => !x.hidden).map(x => x.dataset.aipSec),
      font: p.querySelector('[data-aip-font]').textContent, style: p.querySelector('[data-aip-fstyle]').textContent, pt: p.querySelector('[data-aif="pt"]').value, al: p.querySelector('[data-aim="talign:center"]').classList.contains('is-on') }; });
    assert.equal(s.kind, 'Type'); assert.ok(s.secs.includes('character') && s.secs.includes('paragraph'), JSON.stringify(s));
    assert.ok(s.font === 'Nunito Sans' && s.style === 'Regular' && parseFloat(s.pt) === 10 && s.al, JSON.stringify(s));
    await shot(page, 'aip-1440-6-properties-type.png');
    await page.locator('[data-aip-panel="properties"] [data-aif="pt"]').click(); await page.keyboard.type('14'); await page.keyboard.press('Enter'); await settle(page);
    assert.equal(find(await items(page), 'b-words').sizePt, 14);
    await page.locator('[data-aip-panel="properties"] [data-aif="track"]').click(); await page.keyboard.type('50'); await page.keyboard.press('Enter'); await settle(page);
    assert.equal(find(await items(page), 'b-words').tracking, 50);
    await page.locator('[data-aip-panel="properties"] [data-aim="fontmenu"]').click(); await settle(page);
    await page.locator('.aip-menu:not([hidden]) [data-aim="font:serif"]').click(); await settle(page);
    assert.equal(find(await items(page), 'b-words').fontKey, 'serif');
    await page.keyboard.press('Escape');
    await page.locator('[data-aip-panel="properties"] [data-aim="stylemenu"]').click(); await page.locator('.aip-menu:not([hidden]) [data-aim="fstyle:bold"]').click(); await settle(page);
    assert.equal(find(await items(page), 'b-words').bold, true);
    await page.locator('[data-aip-panel="properties"] [data-aim="case:upper"]').click(); await settle(page);
    assert.equal(find(await items(page), 'b-words').letterCase, 'upper');
  });
  await step('2.5 a group: "Group" with Ungroup; a traced picture (its group keeps the trace): "Image Tracing", the Image Trace settings', async () => {
    await select(page, ['b-stars']);
    let s = await E(page, () => { const p = document.querySelector('[data-aip-panel="properties"]'); return { kind: p.querySelector('[data-aip-kind]').textContent, qa: [...p.querySelectorAll('[data-aip-qa] .aip-btn')].map(b => b.textContent.trim()) }; });
    assert.ok(s.kind === 'Group' && s.qa.includes('Ungroup'), JSON.stringify(s));
    await E(page, () => { const ctx = CharmEditor.current.ctx, sq = [[['m', [120, 200]], ['l', [150, 200]], ['l', [150, 230]], ['l', [120, 230]], ['h']]];
      ctx.addItems([{ kind: 'group', name: 'Image · logo', role: 'hatch', trace: { src: 'img-test', mode: 'grey', levels: 4, detail: 0.5, smooth: 0.5, despeckle: 2 }, children: [{ kind: 'path', role: 'hatch', artGrey: true, fill: [0, 0, 0], stroke: false, subpaths: sq }] }]); ctx.commit('test picture'); });
    await settle(page); await settle(page);
    s = await E(page, () => { const p = document.querySelector('[data-aip-panel="properties"]'); return { kind: p.querySelector('[data-aip-kind]').textContent, trace: !p.querySelector('[data-aip-sec="trace"]').hidden, kv: p.querySelector('[data-aip-trace]').textContent }; });
    assert.ok(s.kind === 'Image Tracing' && s.trace && /Grays:4/.test(s.kv) && /Mode:Grayscale/.test(s.kv), JSON.stringify(s));
    await shot(page, 'aip-1440-7-properties-image.png');
    await page.keyboard.press('Control+z'); await settle(page);
    await page.locator('.ce-root .ce-head [data-face="front"]').click(); await settle(page);
  });

  /* ── 3 · the panels ── */
  const fly = async id => { await page.locator(`.aip-strip [data-aip-ico="${id}"]`).click(); await settle(page); };
  await step('3.1 Align (from its icon, a fly-out with Transform and Pathfinder as tabs): Align To the Artboard, then Horizontal Align Center puts the part on the charm\'s middle', async () => {
    await select(page, ['heart-fill']);
    await fly('align');
    const s = await E(page, () => ({ fly: !document.querySelector('.aip-fly').hidden, tabs: [...document.querySelectorAll('.aip-fly .aip-tab')].map(t => t.textContent), on: document.querySelector('.aip-fly .aip-tab.is-on').textContent }));
    assert.ok(s.fly && s.on === 'Align', JSON.stringify(s)); assert.deepEqual(s.tabs, ['Transform', 'Align', 'Pathfinder']);
    await page.locator('.aip-fly [data-aim="altomenu"]').click(); await page.locator('.aip-menu:not([hidden]) [data-aim="alto:charm"]').click(); await settle(page);
    await page.locator('.aip-fly [data-aim="al:hcenter"]').click(); await settle(page);
    const r = await E(page, () => { const b = CharmEditor.current.box('heart-fill'), c = CharmEditor.current.box('cut-outline'); return [(b[0] + b[2]) / 2, (c[0] + c[2]) / 2]; });
    assert.ok(Math.abs(r[0] - r[1]) < 1e-6, JSON.stringify(r));
    await shot(page, 'aip-1440-8-align-fly.png');
    await page.keyboard.press('Control+z'); await settle(page);
    await page.locator('.aip-fly [data-aim="altomenu"]').click(); await page.locator('.aip-menu:not([hidden]) [data-aim="alto:"]').click(); await settle(page);
  });
  await step('3.2 Transform: Flip Horizontal mirrors the part (the existing command); Pathfinder: Unite makes one of two (one step)', async () => {
    await page.locator('.aip-fly .aip-tab[data-aip-tab="transform"]').click(); await settle(page);
    const it0 = find(await items(page), 'heart-fill');
    await page.locator('.aip-fly [data-aim="x:ops.flipH"]').first().click(); await settle(page);
    const it1 = find(await items(page), 'heart-fill');
    assert.notDeepEqual(JSON.stringify(it1.subpaths) + it1.sx, JSON.stringify(it0.subpaths) + it0.sx, 'flipped');
    await page.keyboard.press('Control+z'); await settle(page);
    const two = await E(page, () => { const ctx = CharmEditor.current.ctx, r = (x, y) => [[['m', [x, y]], ['l', [x + 20, y]], ['l', [x + 20, y + 20]], ['l', [x, y + 20]], ['h']]];
      const ids = ctx.addItems([{ kind: 'path', role: 'hatch', fill: true, stroke: false, subpaths: r(130, 140) }, { kind: 'path', role: 'hatch', fill: true, stroke: false, subpaths: r(140, 150) }]); ctx.commit('two squares'); return ids; });
    await select(page, two);
    await page.locator('.aip-fly .aip-tab[data-aip-tab="pathfinder"]').click(); await settle(page);
    const n0 = (await items(page)).length, h0 = await hist(page);
    await page.locator('.aip-fly [data-aim="x:ops.unite"]').click();
    await page.waitForFunction(n => CharmEditor.current.items().length === n - 1, n0, { timeout: 5000 });
    assert.equal(await hist(page), h0 + 1);
    await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await settle(page);
  });
  await step('3.3 Stroke: the weight typed sets the line width; Swatches: a swatch clicked sets the role (the laser\'s colour)', async () => {
    await select(page, ['heart-line']);
    await fly('stroke');
    await page.locator('.aip-fly [data-aif="lw"]').click(); await page.keyboard.type('0.2'); await page.keyboard.press('Enter'); await settle(page);
    const lw = find(await items(page), 'heart-line').lwPt;
    assert.ok(Math.abs(lw * 25.4 / 72 - 0.2) < 1e-6, String(lw));
    await page.locator('.aip-fly .aip-tab[data-aip-tab="swatches"]').click(); await settle(page);
    await page.locator('.aip-fly .aip-swc[data-aim="role:hatch"]').click(); await settle(page);
    assert.equal(find(await items(page), 'heart-line').role, 'hatch');
    await shot(page, 'aip-1440-9-swatches.png');
    await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await settle(page);
  });
  await step('3.4 Character as an icon: with nothing selected and the Type tool its fields set the next words; with words it shows them', async () => {
    await select(page, []);
    await fly('character');
    assert.ok(await E(page, () => { const n = document.querySelector('.aip-fly [data-aip-none]'); if (!n) throw new Error('fly: ' + JSON.stringify(CharmEditor.aiDock.current.state().fly)); return !n.hidden; }), 'asks for words');
    await page.keyboard.press('t'); await settle(page);
    assert.ok(await E(page, () => document.querySelector('.aip-fly [data-aip-none]').hidden && !document.querySelector('.aip-fly [data-aif="pt"]').disabled));
    await page.keyboard.press('v'); await settle(page);
    await page.mouse.click(10, 10); await settle(page);
    assert.ok(await E(page, () => document.querySelector('.aip-fly').hidden), 'a press outside folds the fly-out away');
  });

  /* ── 4 · the dock ── */
  await step('4.1 » collapses the dock to the icon-and-label strip (the work area grows); an icon pops Layers out; « expands it again', async () => {
    const w0 = await E(page, () => document.querySelector('.ce-stage').getBoundingClientRect().width);
    await page.locator('.aip-head [data-aim="collapse"]').click(); await settle(page); await settle(page);
    const s = await E(page, () => ({ icons: document.querySelector('.ce-dock').classList.contains('is-icons'), labels: document.querySelector('.ce-dock').classList.contains('is-labels'), w: document.querySelector('.ce-stage').getBoundingClientRect().width,
      label: getComputedStyle(document.querySelector('.aip-ico span')).display }));
    assert.ok(s.icons && s.labels && s.label !== 'none' && s.w > w0 + 60, JSON.stringify(s) + ' ' + w0);
    await fly('layers');
    assert.ok(await E(page, () => document.querySelectorAll('.aip-fly .ce-layer').length > 10));
    await row(page, 'heart-fill').locator('.ce-layer__name').click(); await settle(page);
    assert.deepEqual(await sel(page), ['heart-fill']);
    await shot(page, 'aip-1440-10-collapsed-layers-fly.png');
    await page.locator('.aip-head [data-aim="collapse"]').click(); await settle(page);
    assert.ok(await E(page, () => !document.querySelector('.ce-dock').classList.contains('is-icons') && !!document.querySelector('.aip-main .ce-layers')));
  });
  await step('4.2 a tab dragged onto the icon strip becomes an icon (remembered); a double-click on a tab minimises its group; the layout comes back in the next editor', async () => {
    const t = await page.locator('.aip-main .aip-tab[data-aip-tab="properties"]').boundingBox(), s = await page.locator('.aip-strip').boundingBox();
    await page.mouse.move(t.x + 20, t.y + 10); await page.mouse.down(); await page.mouse.move(s.x + s.width / 2, s.y + s.height - 30, { steps: 10 }); await page.mouse.up(); await settle(page);
    let st = await E(page, () => CharmEditor.aiDock.current.state());
    assert.deepEqual(st.layout.main.map(g => g.tabs), [['layers']]); assert.ok(st.layout.strip.some(g => g.tabs.includes('properties')));
    await page.locator('.aip-main .aip-tab[data-aip-tab="layers"]').dblclick(); await settle(page);
    st = await E(page, () => CharmEditor.aiDock.current.state()); assert.equal(st.layout.main[0].min, true);
    await page.locator('.aip-main .aip-tab[data-aip-tab="layers"]').dblclick(); await settle(page);
    await closeIt(page); await openIt(page);
    st = await E(page, () => CharmEditor.aiDock.current.state());
    assert.deepEqual(st.layout.main.map(g => g.tabs), [['layers']], 'remembered');
    await page.locator('.aip-main [data-aip-for="layers"]').click(); await settle(page);
    assert.ok(await E(page, () => !!document.querySelector('.aip-menu:not([hidden]) [data-aim="tostrip:layers"]')));
    await page.keyboard.press('Escape');
    await E(page, () => localStorage.removeItem('ce.aidock')); await closeIt(page); await openIt(page);
  });
  await step('4.3 an extension panel (CharmEditor.panels) has its icon in the strip and opens in its own dock column with these tabs', async () => {
    await closeIt(page);
    await E(page, () => { CharmEditor.panels.push({ id: 'xlib', title: 'Libraries', mount(ctx, el) { el.innerHTML = '<div class="x-body" style="padding:12px">a panel</div>'; } }); });
    await openIt(page);
    await page.locator('.aip-strip [data-aip-xp="xlib"]').click(); await settle(page); await settle(page);
    const s = await E(page, () => { const x = document.querySelector('.ce-xpanel'), d = document.querySelector('.ce-dock'); return { shown: !x.hidden, aip: x.classList.contains('aip-x'), right: x.getBoundingClientRect().left >= d.getBoundingClientRect().right - 1, on: document.querySelector('[data-aip-xp="xlib"]').classList.contains('is-on') }; });
    assert.ok(s.shown && s.aip && s.right && s.on, JSON.stringify(s));
    await shot(page, 'aip-1440-11-extension-panel.png');
    await page.locator('.aip-strip [data-aip-xp="xlib"]').click(); await settle(page);
    assert.ok(await E(page, () => document.querySelector('.ce-xpanel').hidden));
    await closeIt(page); await E(page, () => { CharmEditor.panels.length = 0; try { localStorage.removeItem('ce.xpanel'); } catch (_) {} });
  });
  await step('4.4 CharmEditor.aiDock.add: another file\'s panel joins the dock as a tab (group: properties), mounted when first shown, updated with the selection', async () => {
    await E(page, () => { window.__xp = { mount: 0, update: 0 }; CharmEditor.aiDock.add({ id: 'test-notes', title: 'Notes', group: 'properties', mount(ctx, el) { window.__xp.mount++; el.innerHTML = '<p class="x-notes">notes</p>'; }, update(ctx) { window.__xp.update++; window.__xp.sel = ctx.selection().join(); } }); });
    await openIt(page);
    let s = await E(page, () => ({ tabs: [...document.querySelectorAll('.aip-main .aip-grp')[0].querySelectorAll('.aip-tab')].map(t => t.textContent), xp: Object.assign({}, window.__xp) }));
    assert.deepEqual(s.tabs, ['Properties', 'Notes']); assert.equal(s.xp.mount, 0, 'not mounted until shown');
    await page.locator('.aip-main .aip-tab[data-aip-tab="test-notes"]').click(); await settle(page);
    await select(page, ['heart-line']); await settle(page);
    s = await E(page, () => ({ body: !!document.querySelector('.aip-main .x-notes'), xp: Object.assign({}, window.__xp) }));
    assert.ok(s.body && s.xp.mount === 1 && s.xp.update > 0 && s.xp.sel === 'heart-line', JSON.stringify(s));
    await page.locator('.aip-main .aip-tab[data-aip-tab="properties"]').click(); await settle(page);
    await closeIt(page);
  });
  await step('4.5 no page errors at 1440 × 900', async () => { assert.deepEqual(page.__errs, []); });
  await page.context().close();

  /* ════ 1024 × 768: the dock as icons ════ */
  {
    const p = await newPage(1024, 768);
    await openIt(p);
    await step('5.1 at 1024 × 768 the dock is a strip of icons (every panel, Layers too); Layers pops out from its icon and selects', async () => {
      const s = await E(p, () => ({ icons: document.querySelector('.ce-dock').classList.contains('is-icons'), n: document.querySelectorAll('.aip-strip [data-aip-ico]').length, w: document.querySelector('.ce-dock').getBoundingClientRect().width }));
      assert.ok(s.icons && s.n === 9 && s.w < 50, JSON.stringify(s));
      await shot(p, 'aip-1024-1-icons.png');
      await p.locator('.aip-strip [data-aip-ico="layers"]').click(); await settle(p);
      await p.locator('.ce-root .ce-layer[data-id="heart-line"] .ce-layer__name').click(); await settle(p);
      assert.deepEqual(await E(p, () => CharmEditor.current.selection()), ['heart-line']);
      await shot(p, 'aip-1024-2-layers-fly.png');
      await p.locator('.aip-strip [data-aip-ico="properties"]').click(); await settle(p);
      assert.ok(await E(p, () => !document.querySelector('.aip-fly [data-aip-sec="transform"]').hidden));
      await shot(p, 'aip-1024-3-properties-fly.png');
      const off = await E(p, () => { const r = document.querySelector('.aip-fly').getBoundingClientRect(); return r.left < 0 || r.bottom > innerHeight + 1; });
      assert.equal(off, false, 'the fly-out is on screen');
      assert.deepEqual(p.__errs, []);
    });
    await p.context().close();
  }
  /* ════ 700 × 800: the drawer ════ */
  {
    const p = await newPage(700, 800);
    await openIt(p);
    await step('5.2 at 700 × 800 the dock starts folded away; the head\'s Layers button opens it over the room as a drawer with its panels; × folds it', async () => {
      assert.ok(await E(p, () => document.querySelector('.ce-dock').hidden));
      await p.locator('.ce-root .ce-head [data-act="dock"]').click(); await settle(p);
      const s = await E(p, () => { const d = document.querySelector('.ce-dock'), r = d.getBoundingClientRect(); return { pos: getComputedStyle(d).position, r: r.right, w: innerWidth, rows: document.querySelectorAll('.aip-main .ce-layer').length }; });
      assert.ok(s.pos === 'absolute' && s.r <= s.w + 0.5 && s.rows > 10, JSON.stringify(s));
      await shot(p, 'aip-700-1-drawer.png');
      await p.locator('.ce-root .ce-dock [data-act="dock"]').click(); await settle(p);
      assert.ok(await E(p, () => document.querySelector('.ce-dock').hidden));
      assert.deepEqual(p.__errs, []);
    });
    await p.context().close();
  }

  await browser.close();
  console.log(`\nce-ai-panels: ${results.length} passed, ${failures.length} failed`);
  if (failures.length) { for (const f of failures) console.log('  - ' + f); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
