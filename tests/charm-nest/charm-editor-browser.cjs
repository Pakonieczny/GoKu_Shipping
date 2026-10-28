// THE CHARM EDITOR IN A REAL BROWSER (charm-nest-charm-editor.js; Paul, 27 Oct 2026: "e.g. add a date inside a given calendar day square and update the month name
// located on top of the calendar charm. This means the user needs to be able to zoom into a given charm while maintaining full vector file details and have very fine
// and accurate control when adding text"). One real Chromium page that loads nothing but the editor and a calendar charm (fixtures/charm-editor-harness.html); no
// server, no network, no live service. Everything is done the way a person does it: the mouse, the wheel, the keys, the fields.
//   PW_DIR=/opt/node-tools/node_modules CHROMIUM=/opt/pw-browsers/chromium-1243/chrome-linux64/chrome node tests/charm-nest/charm-editor-browser.cjs
//   (SHOTS=<dir> keeps the screenshots)
// 1 the owner's scenario at 1280 × 720: sweep the month's letters with the marquee and delete them; type SEPTEMBER where they were and centre it with the X field;
//   zoom into one day square with the wheel (it stays under the cursor); type 25, set 5 pt, move it into the square with the arrow keys (0.05 mm, Shift 1 mm: the
//   taps are one undo step); the result's changes (6 removed, 2 added as outlines, nothing else); undo and redo all the way and back; 2000 %: still SVG, no bitmap
//   anywhere, nothing scaled as a picture; Save resolves with the same result; no key reached the page under the editor
// 2 the rest: ⌘A, Esc, Shift-click, group / ungroup, resize (Shift keeps the ratio, Alt from the centre), rotate (Shift: 15°), nudge, copy / paste (0.5 mm) / paste
//   in place / duplicate, the context menu, the Layers panel (eye, lock, select, drag to reorder), the locked cut line, smart guides to the charm's centre, the back
//   face (double-click to edit words, a group whole and Alt inside it), Cancel asks before throwing changes away
// 3 at phone width (390 × 844): nothing overlaps, nothing is cut off, the Layers fold over the room and back
// 4 the app's own font engine is the one used when it is given, and lockRoles is honoured
// 5 the layout at 1280 × 720 with a text selected (the busiest bar): nothing overlaps, the room keeps most of the width
// 6 weight and line width (Paul, 27 Oct 2026: "the thickness of the Red engrave line is too thick, I need the ability to control the thickness of fonts as the current 'pt' control does
//   not correctly control the boldness of the text. I need fine font thickness control over each font"): the Weight field beside the size (one row), typed and stepped by 0.01 mm with
//   the arrows (a run is one undo step), outlines growing with every step and drawn where they were, thinner than the font, its icon back to the font's own; Line engrave takes a
//   Stroke width instead, a group of lines thinned together; the next words armed with a weight; and a line drawn at its TRUE width at every zoom (lwPt × px per pt, counted in the
//   screen's own pixels), with only a hairline floor
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const HARNESS = 'file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html');
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 14).join('\n         ')); };
async function step(name, fn) { try { await fn(); pass(name); } catch (e) { fail(name, e); } }
const near = (a, b, eps) => Math.abs(a - b) <= eps;
const LETTERS = Array.from({ length: 6 }, (_, i) => 'm-letter-' + i);

/* in the page */
const H = (page, fn, arg) => page.evaluate(fn, arg);
const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const cur = (page, expr) => page.evaluate(new Function('return (function(C, F){ return ' + expr + '; })(window.CharmEditor.current, window.fixture);'));
const ptClient = (page, x, y) => page.evaluate(([x, y]) => window.CharmEditor.current.clientOf(x, y), [x, y]);
const mmPt = (page, xmm, ymm) => page.evaluate(([x, y]) => [window.fixture.X0 + x * window.fixture.PT, window.fixture.Y0 + y * window.fixture.PT], [xmm, ymm]);
const box = (page, id) => page.evaluate(id => window.CharmEditor.current.box(id), id);
const items = page => page.evaluate(() => window.CharmEditor.current.items());
const findText = (page, text) => page.evaluate(t => { let f = null; const walk = l => l.forEach(i => { if (i.kind === 'text' && i.text === t) f = i; if (i.children) walk(i.children); }); walk(window.CharmEditor.current.items()); return f; }, text);
async function drag(page, a, b, opts) {
  opts = opts || {};
  for (const k of opts.keys || []) await page.keyboard.down(k);
  await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: opts.steps || 10 }); await page.mouse.up();
  for (const k of (opts.keys || []).slice().reverse()) await page.keyboard.up(k);
  await settle(page);
}
/** page.mouse.click has no modifiers of its own: Shift / Alt are held on the keyboard around it, as a hand does. */
async function clickAt(page, p, opts) {
  opts = Object.assign({}, opts); const mods = opts.modifiers || []; delete opts.modifiers;
  for (const k of mods) await page.keyboard.down(k);
  await page.mouse.click(p.x, p.y, opts);
  for (const k of mods.slice().reverse()) await page.keyboard.up(k);
  await settle(page);
}
async function typeField(page, f, value) {
  const el = page.locator(`.ce-root [data-f="${f}"]`);
  await el.click(); await page.keyboard.press('Control+a'); await page.keyboard.type(String(value)); await page.keyboard.press('Enter'); await settle(page);
}
async function shot(page, name) { if (!SHOTS) return null; const p = path.join(SHOTS, name); await page.screenshot({ path: p }); return p; }
/** A point on a day square's left edge (squares are picked by their line, as in Illustrator). */
async function edgeOf(page, id, at) { const b = await box(page, id); return ptClient(page, b[0], b[1] + (b[3] - b[1]) * (at == null ? 0.5 : at)); }

/** Nothing overlapping and nothing off screen or cut off, among the controls a person sees (and the regions they sit in). */
function layoutIssues() {
  const root = document.querySelector('.ce-root'); if (!root) return ['no editor'];
  const vis = el => { if (!el) return false; const r = el.getBoundingClientRect(), cs = getComputedStyle(el); return r.width > 0.5 && r.height > 0.5 && cs.visibility !== 'hidden' && cs.display !== 'none' && !el.closest('[hidden]'); };
  const R = el => el.getBoundingClientRect();
  const over = (a, b) => Math.min(a.right, b.right) - Math.max(a.left, b.left) > 0.75 && Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top) > 0.75;
  const name = el => (el.className && el.className.baseVal == null ? String(el.className).split(' ')[0] : el.tagName) + (el.dataset && (el.dataset.tool || el.dataset.act || el.dataset.f || el.dataset.drop || el.dataset.face) ? '[' + (el.dataset.tool || el.dataset.act || el.dataset.f || el.dataset.drop || el.dataset.face) + ']' : '');
  const issues = [], vw = innerWidth, vh = innerHeight;
  const check = (label, els) => {
    els = els.filter(vis);
    for (let i = 0; i < els.length; i++) {
      const a = R(els[i]);
      if (a.left < -0.5 || a.top < -0.5 || a.right > vw + 0.5 || a.bottom > vh + 0.5) issues.push(`${label}: ${name(els[i])} off screen ${Math.round(a.left)},${Math.round(a.top)},${Math.round(a.right)},${Math.round(a.bottom)}`);
      for (let j = i + 1; j < els.length; j++) if (over(a, R(els[j]))) issues.push(`${label}: ${name(els[i])} overlaps ${name(els[j])}`);
    }
  };
  check('head', [...root.querySelectorAll('.ce-head > *')]);
  check('rail', [...root.querySelectorAll('.ce-rail .ce-tool, .ce-hist__b')]);
  const props = root.querySelector('.ce-props');
  check('props', [...props.children].flatMap(el => (getComputedStyle(el).display === 'contents' ? [...el.children] : [el])).filter(el => !el.classList.contains('ce-grow')));
  check('size', [...root.querySelectorAll('.ce-size__f, .ce-size__link, .ce-size__u, .ce-size__sep')]);
  check('foot', [...root.querySelectorAll('.ce-foot .ce-zoomb, .ce-hint, .ce-actions .ce-btn')]);
  const dock = root.querySelector('.ce-dock'), docked = vis(dock) && getComputedStyle(dock).position !== 'absolute';
  check('regions', ['.ce-head', '.ce-rail', '.ce-props', '.ce-wrap', '.ce-foot'].map(s => root.querySelector(s)).concat(docked ? [dock] : []));
  if (vis(dock)) { check('dock bar', [...dock.querySelectorAll('.ce-pane__bar > .ce-act')]); check('dock tabs', [...dock.querySelectorAll('.ce-dock__tabs > *')]); check('dock', [dock]); }
  for (const el of root.querySelectorAll('.ce-btn, .ce-prop, .ce-seg button, .ce-dock__tab, .ce-icon')) if (vis(el) && el.scrollWidth > el.clientWidth + 1) issues.push('cut off: ' + name(el) + ' "' + el.textContent.trim() + '"');
  return issues;
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const shots = [];
  const newPage = async (w, h) => {
    const ctx = await browser.newContext({ viewport: { width: w, height: h }, reducedMotion: 'reduce' });
    await ctx.route(u => !u.href.startsWith('file://'), r => r.abort());   // nothing leaves the machine
    const page = await ctx.newPage();
    page.__errs = [];
    page.on('pageerror', e => page.__errs.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') page.__errs.push('console: ' + m.text()); });
    await page.goto(HARNESS);
    return page;
  };
  const openIt = async (page, extra) => { await page.evaluate(x => { window.openEditor(x || undefined); }, extra || null); await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page); };

  /* ════ 1 · the owner's scenario ════ */
  {
    const page = await newPage(1280, 720);
    await openIt(page);
    await step('1.0 opens over the page: a full-window layer with the charm drawn as SVG paths, cut lines locked, the rest selectable', async () => {
      const s = await H(page, () => ({ paths: document.querySelectorAll('.ce-items path').length, leaves: 45, root: !!document.querySelector('.ce-root'), zoom: window.CharmEditor.current.zoom(), cut: window.CharmEditor.current.items().filter(i => i.locked).map(i => i.id) }));
      assert.equal(s.paths, 45); assert.ok(s.root); assert.ok(s.zoom > 300 && s.zoom < 500, 'fitted: ' + s.zoom);
      assert.deepEqual(s.cut.sort(), ['cut-hole', 'cut-outline']);
    });
    await step('1.0b at 1280 × 720 nothing overlaps or is cut off; the room keeps most of the window', async () => {
      const issues = await H(page, layoutIssues); assert.deepEqual(issues, []);
      const w = await H(page, () => document.querySelector('.ce-stage').getBoundingClientRect().width);
      assert.ok(w >= 1280 * 0.6, 'stage width ' + w);
      shots.push(await shot(page, 'desk-1-open.png'));
    });
    await step('1.0c keys pressed in the editor never reach the page under it', async () => {
      await page.keyboard.press('v'); await page.keyboard.press('ArrowLeft'); await page.keyboard.press('a');
      assert.equal(await H(page, () => window.__pageKeys), 0);
    });

    let lettersBox = null;
    await step('1.1 the marquee sweeps the month\'s six letters (and nothing else) and Delete takes them out', async () => {
      await page.keyboard.press('m'); assert.equal(await cur(page, 'C.tool()'), 'marquee');
      lettersBox = await H(page, ids => { let b = null; for (const id of ids) { const x = window.CharmEditor.current.box(id); b = b ? [Math.min(b[0], x[0]), Math.min(b[1], x[1]), Math.max(b[2], x[2]), Math.max(b[3], x[3])] : x; } return b; }, LETTERS);
      const pad = 0.5 * 72 / 25.4;
      const a = await ptClient(page, lettersBox[0] - pad, lettersBox[1] - pad), b = await ptClient(page, lettersBox[2] + pad, lettersBox[3] + pad);
      await drag(page, a, b);
      assert.deepEqual((await cur(page, 'C.selection()')).sort(), LETTERS.slice().sort());
      shots.push(await shot(page, 'desk-2-marquee.png'));
      await page.keyboard.press('Delete'); await settle(page);
      const ids = (await items(page)).map(i => i.id);
      assert.ok(LETTERS.every(id => !ids.includes(id)) && ids.length === 39, 'left: ' + ids.length);
      assert.deepEqual(await cur(page, 'C.selection()'), []);
    });
    await step('1.2 the text tool clicks to place: SEPTEMBER typed where AUGUST was, then centred with the X field (a position, in mm)', async () => {
      await page.keyboard.press('t');
      const at = await H(page, () => window.CharmEditor.current.clientOf(window.fixture.month.startX, window.fixture.month.baseY));
      await clickAt(page, at);
      assert.ok(await H(page, () => !!document.querySelector('.ce-editor') && document.activeElement === document.querySelector('.ce-editor')), 'the words box is open and focused');
      await page.keyboard.type('SEPTEMBER'); await page.keyboard.press('Enter'); await settle(page);
      const t = await findText(page, 'SEPTEMBER');
      assert.ok(t && t.source === 'added' && t.kind === 'text', 'a text item');
      assert.deepEqual(await cur(page, 'C.selection()'), [t.id]);
      assert.equal(await cur(page, 'C.tool()'), 'text');
      const w = parseFloat(await page.locator('.ce-root [data-f="w"]').inputValue());
      const fields = () => H(page, () => ['x', 'y', 'w', 'h', 'r'].map(f => document.querySelector(`.ce-root [data-f="${f}"]`).value).join(' '));
      const f0 = await fields();
      await typeField(page, 'x', ((24 - w) / 2).toFixed(3));
      const b = await box(page, t.id), mid = await mmPt(page, 12, 0);
      assert.ok(near((b[0] + b[2]) / 2, mid[0], 0.01 * 72 / 25.4), `centred on the charm: ${(b[0] + b[2]) / 2 - mid[0]} (w ${w}, fields ${f0} -> ${await fields()})`);
      assert.ok(b[3] < lettersBox[3] + 0.6 && b[1] > lettersBox[1] - 1.5, 'on the line the month was on');
      shots.push(await shot(page, 'desk-3-september.png'));
    });

    let sq = null, zoomBefore = 0;
    await step('1.3 the wheel zooms into one day square about the cursor: the square stays under the hand, past 2000 % (and letting go of the words, which folds the bar back to one row, does not move the charm on screen)', async () => {
      const rows = () => H(page, () => { const t = [...document.querySelector('.ce-props').children].filter(e => e.getBoundingClientRect().height > 0).map(e => Math.round(e.getBoundingClientRect().top)); return new Set(t).size; });
      const corner = () => H(page, () => { const f = window.CharmEditor.current.frame(); return window.CharmEditor.current.clientOf(f.x0, f.y0); });
      const r0 = await rows(), k0 = await corner();
      assert.equal(r0, 1, 'the bar is one row with the words selected (font, size, weight, role)');
      await page.keyboard.press('Escape'); await page.keyboard.press('v'); await settle(page); await settle(page);
      const r1 = await rows(), k1 = await corner();
      assert.ok(near(k1.x, k0.x, 0.5) && near(k1.y, k0.y, 0.5), `the charm stayed put (${r0} -> ${r1} rows): ${JSON.stringify([k0, k1])}`);
      sq = await box(page, 'day-2-3');
      const cc = await ptClient(page, (sq[0] + sq[2]) / 2, (sq[1] + sq[3]) / 2), c0 = { x: Math.round(cc.x), y: Math.round(cc.y) };   // (a wheel event lands on whole pixels)
      zoomBefore = await cur(page, 'C.zoom()');
      const p0 = await H(page, c => window.CharmEditor.current.ptOf(c.x, c.y), c0);
      await page.mouse.move(c0.x, c0.y);
      for (let i = 0; i < 10; i++) { await page.mouse.wheel(0, -100); await page.waitForTimeout(16); }
      await settle(page);
      const z = await cur(page, 'C.zoom()'), p1 = await H(page, c => window.CharmEditor.current.ptOf(c.x, c.y), c0), c1 = await ptClient(page, (sq[0] + sq[2]) / 2, (sq[1] + sq[3]) / 2);
      assert.ok(z > 2000 && z <= 4000, 'zoom ' + z + ' from ' + zoomBefore);
      assert.ok(near(p1[0], p0[0], 1e-6) && near(p1[1], p0[1], 1e-6), `the point under the cursor stays under it: ${p0} -> ${p1}`);
      assert.ok(near(c1.x, c0.x, 4) && near(c1.y, c0.y, 4), `the square's middle is still at the cursor: ${c0.x},${c0.y} -> ${c1.x},${c1.y}`);
      assert.equal(await H(page, () => document.querySelector('[data-zoomread]').textContent), Math.round(z) + '%');
      shots.push(await shot(page, 'desk-4-zoomed-square.png'));
    });

    let t25 = null, before25 = null, undoBefore = 0, size0 = 0;
    await step('1.4 "25" is typed beside the square, set to 5 pt in the bar, and moved into the square with the arrow keys (Shift 1 mm, then 0.05 mm taps)', async () => {
      await page.keyboard.press('t');
      const at = await ptClient(page, sq[0] - 1.2 * 72 / 25.4, sq[3] - 0.5 * 72 / 25.4);
      await clickAt(page, at);
      await page.keyboard.type('25'); await page.keyboard.press('Enter'); await settle(page);
      t25 = await findText(page, '25'); assert.ok(t25, 'the 25');
      size0 = t25.sizePt;
      await typeField(page, 'pt', '5');
      t25 = await findText(page, '25'); assert.equal(t25.sizePt, 5);
      before25 = await box(page, t25.id);
      undoBefore = (await cur(page, 'C.history()')).undo;
      const tb = before25;
      assert.deepEqual(await cur(page, 'C.selection()'), [t25.id], 'the field gave the keys back with the 25 still selected');
      const PT = 72 / 25.4, mm = v => v / PT;
      const dx = mm((sq[0] + sq[2]) / 2 - (tb[0] + tb[2]) / 2), dy = mm((sq[1] + sq[3]) / 2 - (tb[1] + tb[3]) / 2);
      const keys = (d, pos, neg) => { const big = Math.trunc(d), small = Math.round((d - big) / 0.05); return [...Array(Math.abs(big)).fill('Shift+' + (big > 0 ? pos : neg)), ...Array(Math.abs(small)).fill(small > 0 ? pos : neg)]; };
      const seq = keys(dx, 'ArrowRight', 'ArrowLeft').concat(keys(dy, 'ArrowDown', 'ArrowUp'));
      assert.ok(seq.length > 5, 'taps: ' + seq.length);
      for (const k of seq) await page.keyboard.press(k);
      await settle(page);
      const b = await box(page, t25.id);
      const inner = [sq[0] + 0.15, sq[1] + 0.15, sq[2] - 0.15, sq[3] - 0.15];
      assert.ok(b[0] > inner[0] && b[1] > inner[1] && b[2] < inner[2] && b[3] < inner[3], 'inside the square: ' + JSON.stringify([b, inner]));
      assert.ok(near(mm((b[0] + b[2]) / 2 - (sq[0] + sq[2]) / 2), 0, 0.026) && near(mm((b[1] + b[3]) / 2 - (sq[1] + sq[3]) / 2), 0, 0.026), 'centred to the 0.05 mm step');
      const moved = mm((b[0] + b[2]) / 2 - (tb[0] + tb[2]) / 2);
      assert.ok(near(moved, Math.round(dx / 0.05) * 0.05, 1e-6) || near(moved, Math.trunc(dx) + Math.round((dx - Math.trunc(dx)) / 0.05) * 0.05, 1e-6), 'moved by whole steps: ' + moved);
      assert.equal((await cur(page, 'C.history()')).undo, undoBefore + 1, `${seq.length} taps are ONE undo step`);
      shots.push(await shot(page, 'desk-5-twentyfive.png'));
    });

    let res = null;
    await step('1.5 the result\'s changes: the six letters removed, SEPTEMBER and 25 added as outlines in face space (25 inside its square), nothing else changed; the back untouched', async () => {
      res = await cur(page, 'C.result()');
      const f = res.changes.front, b = res.changes.back;
      assert.deepEqual(f.removed.slice().sort(), LETTERS.slice().sort());
      assert.deepEqual(f.added.map(a => a.text).sort(), ['25', 'SEPTEMBER']);
      assert.deepEqual(Object.keys(f.changed), []);
      assert.deepEqual([b.removed.length, b.added.length, Object.keys(b.changed).length], [0, 0, 0]);
      for (const a of f.added) {
        assert.ok(a.subpaths.length > 0 && a.subpaths.every(sp => sp.every(op => ['m', 'l', 'c', 'h'].includes(op[0]))), 'outlines of ' + a.text);
        const fr = res.faces.front.frame;
        assert.ok(a.bbox[0] > fr.x0 && a.bbox[2] < fr.x0 + fr.wPt && a.bbox[1] > fr.y0 && a.bbox[3] < fr.y0 + fr.hPt, 'in face coordinates: ' + a.text);
      }
      const n25 = f.added.find(a => a.text === '25');
      assert.ok(n25.bbox[0] > sq[0] && n25.bbox[2] < sq[2] && n25.bbox[1] > sq[1] && n25.bbox[3] < sq[3], 'the 25\'s outlines are inside the square');
      assert.ok(res.faces.front.items.every(i => !LETTERS.includes(i.id)) && res.faces.front.items.find(i => i.id === 'cut-outline').locked === false, 'faces: letters gone, the cut line\'s lock is the editor\'s and does not leave');
    });

    await step('1.6 undo and redo: back through the nudges (one step), the size, the 25, the centring, SEPTEMBER and the deletion, then forward to the same charm', async () => {
      const sig = () => page.evaluate(() => JSON.stringify(window.CharmEditor.current.items().map(i => [i.id, i.kind, i.text || '', +(i.tx || 0).toFixed(6), +(i.ty || 0).toFixed(6), i.sizePt || 0])));
      const end = await sig();
      await page.keyboard.press('Control+z'); await settle(page);
      const b = await box(page, t25.id);
      assert.ok(b && b.every((v, i) => near(v, before25[i], 1e-6)), 'the nudges undone in one step');
      await page.keyboard.press('Control+z'); await settle(page);
      assert.equal((await findText(page, '25')).sizePt, size0, 'the size undone');
      assert.notEqual(size0, 5);
      await page.keyboard.press('Control+z'); await settle(page);
      assert.equal(await findText(page, '25'), null, 'the 25 undone');
      for (let i = 0; i < 3; i++) { await page.keyboard.press('Control+z'); await settle(page); }
      const ids = (await items(page)).map(i => i.id);
      assert.ok(LETTERS.every(id => ids.includes(id)) && !(await findText(page, 'SEPTEMBER')), 'back to the charm as it came in');
      assert.equal((await cur(page, 'C.history()')).undo, 0);
      assert.equal(await cur(page, 'C.dirty()'), false, 'nothing to save');
      for (let i = 0; i < 6; i++) { await page.keyboard.press('Control+Shift+z'); await settle(page); }   // (28 Oct 2026: ⌘Y is Illustrator's Outline view now; redo is ⇧⌘Z, as in Illustrator)
      assert.equal(await sig(), end, 'redo arrives at exactly the same charm');
      assert.equal((await cur(page, 'C.history()')).redo, 0);
    });

    await step('1.7 at 2000 % it is still vector: SVG paths in points, no image, canvas or foreignObject in the room, nothing scaled as a picture, every stroke non-scaling', async () => {
      await page.keyboard.press('v');
      await clickAt(page, await H(page, id => { const b = window.CharmEditor.current.box(id); return window.CharmEditor.current.clientOf((b[0] + b[2]) / 2, (b[1] + b[3]) / 2); }, t25.id));
      await page.keyboard.press('Control+2'); await settle(page);
      await page.locator('.ce-root .ce-zoomb').click();
      await page.locator('.ce-root [data-zoom="2000"]').click(); await settle(page);
      const v = await H(page, () => {
        const stage = document.querySelector('.ce-stage'), art = document.querySelector('.ce-art'), r = stage.getBoundingClientRect(), vb = art.getAttribute('viewBox').split(/\s+/).map(Number);
        const paths = [...document.querySelectorAll('.ce-items path')];
        return { zoom: window.CharmEditor.current.zoom(), read: document.querySelector('[data-zoomread]').textContent, raster: stage.querySelectorAll('img, canvas, image, foreignObject, video, picture').length,
          transform: getComputedStyle(art).transform, scaleOk: Math.abs(vb[2] * (window.CharmEditor.current.zoom() / 100 * 96 / 72) - r.width) < 0.5, paths: paths.length,
          nonScaling: paths.every(p => p.getAttribute('vector-effect') === 'non-scaling-stroke'), dOk: paths.every(p => /^[MLCZ0-9 .\-e]+$/.test(p.getAttribute('d'))), bg: [...stage.querySelectorAll('*')].filter(el => /url\(/.test(getComputedStyle(el).backgroundImage)).length };
      });
      assert.ok(near(v.zoom, 2000, 0.01) && v.read === '2000%', 'zoom ' + v.zoom);
      assert.equal(v.raster, 0); assert.equal(v.bg, 0); assert.equal(v.transform, 'none'); assert.ok(v.scaleOk, 'the viewBox carries the zoom');
      assert.equal(v.paths, 41); assert.ok(v.nonScaling && v.dOk);
      shots.push(await shot(page, 'desk-6-2000pct.png'));
    });

    await step('1.8 Save resolves the promise with the same result; the editor is gone, the page scrolls again, no key reached it, no error was logged', async () => {
      await page.locator('.ce-root [data-act="save"]').click();
      await page.waitForFunction(() => window.__closed === true);
      const r = await H(page, () => ({ result: window.__result, gone: !document.querySelector('.ce-root'), overflow: document.documentElement.style.overflow, keys: window.__pageKeys, current: window.CharmEditor.current }));
      assert.ok(r.gone && r.overflow === '' && r.keys === 0 && r.current === null);
      assert.deepEqual(JSON.parse(JSON.stringify(r.result.changes)), JSON.parse(JSON.stringify(res.changes)));
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }

  /* ════ 2 · the rest of the tool set ════ */
  {
    const page = await newPage(1280, 720);
    await openIt(page);
    const PT = 72 / 25.4;
    await step('2.1 ⌘A selects everything not locked (43 of 45: the cut lines are locked); Esc lets go and does not close', async () => {
      await page.keyboard.press('Control+a'); assert.equal((await cur(page, 'C.selection()')).length, 43);
      await page.keyboard.press('Escape'); assert.equal((await cur(page, 'C.selection()')).length, 0);
      assert.ok(await H(page, () => !!document.querySelector('.ce-root')));
    });
    await step('2.2 a click on a day square\'s line selects it, Shift-click adds the next; ⌘G groups them (a group row in Layers), ⇧⌘G takes the group apart', async () => {
      await clickAt(page, await edgeOf(page, 'day-0-0'));
      assert.deepEqual(await cur(page, 'C.selection()'), ['day-0-0']);
      await clickAt(page, await edgeOf(page, 'day-0-1'), { modifiers: ['Shift'] });
      assert.deepEqual((await cur(page, 'C.selection()')).sort(), ['day-0-0', 'day-0-1']);
      await page.keyboard.press('Control+g'); await settle(page);
      const g = await H(page, () => { const it = window.CharmEditor.current.items().find(i => i.kind === 'group'); return it && { id: it.id, kids: it.children.map(c => c.id) }; });
      assert.ok(g && g.kids.join() === 'day-0-0,day-0-1');
      assert.deepEqual(await cur(page, 'C.selection()'), [g.id]);
      assert.ok(await H(page, id => !!document.querySelector(`.ce-layer[data-id="${id}"] .ce-layer__fold`), g.id), 'a group row');
      await clickAt(page, await edgeOf(page, 'day-0-1', 0.3));
      assert.deepEqual(await cur(page, 'C.selection()'), [g.id], 'a click on a member takes the group');
      await clickAt(page, await edgeOf(page, 'day-0-1', 0.3), { modifiers: ['Alt'] });
      assert.deepEqual(await cur(page, 'C.selection()'), ['day-0-1'], 'Alt-click reaches inside');
      await clickAt(page, await edgeOf(page, 'day-0-0')); await page.keyboard.press('Control+Shift+g'); await settle(page);
      assert.ok(await H(page, () => !window.CharmEditor.current.items().some(i => i.kind === 'group')), 'ungrouped');
      assert.deepEqual((await cur(page, 'C.selection()')).sort(), ['day-0-0', 'day-0-1']);
    });
    const handle = (page, h) => page.evaluate(h => { const r = document.querySelector(`.ce-ui [data-h="${h}"]`).getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; }, h);
    await step('2.3 eight handles and a turn grip; Shift keeps the ratio (the opposite corner stays), Alt resizes from the centre', async () => {
      await page.keyboard.press('Escape');
      await clickAt(page, await edgeOf(page, 'day-1-1'));
      await page.keyboard.press('Control+2'); await settle(page);   // zoom to the selection
      assert.ok(await cur(page, 'C.zoom()') > 1000);
      assert.equal(await H(page, () => document.querySelectorAll('.ce-ui [data-h]').length), 9);
      const b0 = await box(page, 'day-1-1');
      const se = await handle(page, 'se');
      await drag(page, se, { x: se.x + 40, y: se.y + 8 }, { keys: ['Shift'] });
      let it = await H(page, () => window.CharmEditor.current.items().find(i => i.id === 'day-1-1'));
      const b1 = await box(page, 'day-1-1');
      assert.ok(near(it.sx, it.sy, 1e-9) && it.sx > 1.05, 'ratio kept: ' + it.sx + ' ' + it.sy);
      assert.ok(near(b1[0], b0[0], 1e-6) && near(b1[1], b0[1], 1e-6), 'the opposite corner stayed');
      const c1 = [(b1[0] + b1[2]) / 2, (b1[1] + b1[3]) / 2];
      const se2 = await handle(page, 'se');
      await drag(page, se2, { x: se2.x + 25, y: se2.y + 4 }, { keys: ['Alt'] });
      const b2 = await box(page, 'day-1-1');
      assert.ok(near((b2[0] + b2[2]) / 2, c1[0], 1e-6) && near((b2[1] + b2[3]) / 2, c1[1], 1e-6), 'Alt: from the centre');
      it = await H(page, () => window.CharmEditor.current.items().find(i => i.id === 'day-1-1'));
      assert.ok(!near(it.sx, it.sy, 1e-3), 'a free resize without Shift');
      const e = await handle(page, 'e'), w0 = b2[2] - b2[0], h0 = b2[3] - b2[1];
      await drag(page, e, { x: e.x - 20, y: e.y + 30 });
      const b3 = await box(page, 'day-1-1');
      assert.ok(near(b3[3] - b3[1], h0, 1e-6) && b3[2] - b3[0] < w0, 'a side handle changes one side only');
    });
    await step('2.4 the turn grip turns about the centre; Shift snaps to 15°; the angle field reads it', async () => {
      const b = await box(page, 'day-1-1'), c = await ptClient(page, (b[0] + b[2]) / 2, (b[1] + b[3]) / 2);
      const r = await handle(page, 'rot');
      const ang = 37 * Math.PI / 180, R = Math.hypot(r.x - c.x, r.y - c.y);
      const to = { x: c.x + Math.sin(ang) * R, y: c.y - Math.cos(ang) * R };
      await drag(page, r, to, { keys: ['Shift'] });
      const it = await H(page, () => window.CharmEditor.current.items().find(i => i.id === 'day-1-1'));
      assert.ok(near(it.rot, 30, 1e-6) || near(it.rot, 45, 1e-6), 'rot ' + it.rot);
      assert.equal(await page.locator('.ce-root [data-f="r"]').inputValue(), String(it.rot));
      const b2 = await box(page, 'day-1-1');
      assert.ok(near((b2[0] + b2[2]) / 2, (b[0] + b[2]) / 2, 1e-6) && near((b2[1] + b2[3]) / 2, (b[1] + b[3]) / 2, 1e-6), 'about its centre');
    });
    await step('2.5 arrows nudge 0.05 mm, Shift + arrow 1 mm (exactly)', async () => {
      const b0 = await box(page, 'day-1-1');
      await page.keyboard.press('ArrowRight'); await page.keyboard.press('Shift+ArrowDown'); await settle(page);
      const b1 = await box(page, 'day-1-1');
      assert.ok(near(b1[0] - b0[0], 0.05 * PT, 1e-6) && near(b1[1] - b0[1], 1 * PT, 1e-6));
      await page.keyboard.press('Control+0'); await settle(page);   // back to the whole charm
    });
    await step('2.6 ⌘C ⌘V pastes 0.5 mm away (again: 1 mm), ⌘D duplicates 0.5 mm from what is selected, ⇧⌘V pastes in place, ⌘X cuts', async () => {
      await page.keyboard.press('Escape'); await clickAt(page, await edgeOf(page, 'day-3-3'));
      const b0 = await box(page, 'day-3-3');
      await page.keyboard.press('Control+c'); await page.keyboard.press('Control+v'); await settle(page);
      const p1 = (await cur(page, 'C.selection()'))[0], bp1 = await box(page, p1);
      assert.ok(p1 !== 'day-3-3' && near(bp1[0] - b0[0], 0.5 * PT, 1e-6) && near(bp1[1] - b0[1], 0.5 * PT, 1e-6), 'paste 0.5 mm');
      await page.keyboard.press('Control+v'); await settle(page);
      const p2 = (await cur(page, 'C.selection()'))[0], bp2 = await box(page, p2);
      assert.ok(near(bp2[0] - b0[0], 1 * PT, 1e-6), 'the next paste 1 mm');
      await page.keyboard.press('Control+d'); await settle(page);
      const d1 = (await cur(page, 'C.selection()'))[0], bd = await box(page, d1);
      assert.ok(d1 !== p2 && near(bd[0] - bp2[0], 0.5 * PT, 1e-6) && near(bd[1] - bp2[1], 0.5 * PT, 1e-6), 'duplicate 0.5 mm');
      await page.keyboard.press('Control+Shift+v'); await settle(page);
      const pin = (await cur(page, 'C.selection()'))[0], bpin = await box(page, pin);
      assert.ok(bpin.every((v, i) => near(v, b0[i], 1e-9)), 'in place');
      const src = await H(page, id => window.CharmEditor.current.items().find(i => i.id === id).source, pin);
      assert.equal(src, 'added');
      const n0 = (await items(page)).length;
      await page.keyboard.press('Control+x'); await settle(page);
      assert.equal((await items(page)).length, n0 - 1);
      await page.keyboard.press('Control+z'); await settle(page);   // the cut comes back
      assert.equal((await items(page)).length, n0);
    });
    await step('2.7 right-click raises the object menu (cut, copy, paste, duplicate, delete, order, group, lock, hide); Hide hides it and it can no longer be clicked', async () => {
      await page.keyboard.press('Escape');
      const p = await edgeOf(page, 'day-4-2');
      await clickAt(page, p, { button: 'right' });
      const labels = await H(page, () => { const m = document.querySelector('.ce-ctx'); return m.hidden ? null : [...m.querySelectorAll('.ce-item')].map(b => b.textContent.replace(/[⌘⇧⌫\[\]]|double-click/g, '').trim()); });
      for (const w of ['Cut', 'Copy', 'Paste', 'Duplicate', 'Delete', 'Bring to front', 'Bring forward', 'Send backward', 'Send to back', 'Group', 'Ungroup', 'Lock', 'Hide']) assert.ok(labels && labels.some(l => l.startsWith(w)), 'menu has ' + w + ': ' + labels);
      assert.deepEqual(await cur(page, 'C.selection()'), ['day-4-2']);
      shots.push(await shot(page, 'desk-7-context.png'));
      await page.locator('.ce-ctx [data-ctx="hide"]').click(); await settle(page);
      assert.equal(await H(page, () => window.CharmEditor.current.items().find(i => i.id === 'day-4-2').hidden), true);
      await clickAt(page, p);
      assert.notDeepEqual(await cur(page, 'C.selection()'), ['day-4-2']);
    });
    await step('2.8 the Layers panel: a row selects (and the canvas follows), the eye hides, the lock locks (then a click on the canvas passes it by), a drag reorders', async () => {
      const row = id => page.locator(`.ce-root .ce-layer[data-id="${id}"]`);
      await row('day-4-0').scrollIntoViewIfNeeded(); await row('day-4-0').locator('.ce-layer__name').click(); await settle(page);
      assert.deepEqual(await cur(page, 'C.selection()'), ['day-4-0']);
      assert.ok(await H(page, () => !!document.querySelector('.ce-ui .u-box')), 'the canvas shows the selection');
      await row('day-4-0').locator('[data-lact="eye"]').click(); await settle(page);
      assert.equal(await H(page, () => window.CharmEditor.current.items().find(i => i.id === 'day-4-0').hidden), true);
      await row('day-4-0').locator('[data-lact="eye"]').click(); await settle(page);
      await row('day-4-1').scrollIntoViewIfNeeded(); await row('day-4-1').locator('[data-lact="lock"]').click(); await settle(page);
      assert.equal(await H(page, () => window.CharmEditor.current.items().find(i => i.id === 'day-4-1').locked), true);
      await page.keyboard.press('Escape');
      await clickAt(page, await edgeOf(page, 'day-4-1'));
      assert.ok(!(await cur(page, 'C.selection()')).includes('day-4-1'), 'a locked layer is not picked on the canvas');
      await row('heart-fill').scrollIntoViewIfNeeded();
      const a = await row('heart-fill').boundingBox(), b = await row('heart-line').boundingBox();
      await drag(page, { x: a.x + 20, y: a.y + a.height / 2 }, { x: b.x + 20, y: b.y + b.height * 0.8 });
      const order = (await items(page)).map(i => i.id);
      assert.ok(order.indexOf('heart-fill') < order.indexOf('heart-line'), 'heart-fill moved below heart-line');
      shots.push(await shot(page, 'desk-8-layers.png'));
    });
    await step('2.9 the cut line opens locked: a click on it passes by; unlocked from its row, it can be picked', async () => {
      await page.keyboard.press('Escape');
      const b = await box(page, 'cut-outline'), p = await ptClient(page, b[0], (b[1] + b[3]) / 2);
      await clickAt(page, p);
      assert.ok(!(await cur(page, 'C.selection()')).includes('cut-outline'));
      const row = page.locator('.ce-root .ce-layer[data-id="cut-outline"]');
      await row.scrollIntoViewIfNeeded(); await row.locator('[data-lact="lock"]').click(); await settle(page);
      await clickAt(page, p);
      assert.deepEqual(await cur(page, 'C.selection()'), ['cut-outline']);
    });
    await step('2.10 smart guides: a square dragged near the charm\'s middle lands on it exactly, and a guide is drawn while it is held (Alt lets it go anywhere)', async () => {
      await page.keyboard.press('Escape');
      // the month's letters have edges a hair from the middle (the U's left edge is 0.05 mm off it), and a guide rightly takes the nearest: they go first
      await page.keyboard.press('m');
      const lb = await H(page, ids => { let b = null; for (const id of ids) { const x = window.CharmEditor.current.box(id); b = b ? [Math.min(b[0], x[0]), Math.min(b[1], x[1]), Math.max(b[2], x[2]), Math.max(b[3], x[3])] : x; } return b; }, LETTERS);
      await drag(page, await ptClient(page, lb[0] - 1, lb[1] - 1), await ptClient(page, lb[2] + 1, lb[3] + 1));
      await page.keyboard.press('Delete'); await page.keyboard.press('v'); await settle(page);
      const b = await box(page, 'day-2-1'), fr = await cur(page, 'C.frame()'), midX = fr.x0 + fr.wPt / 2;
      const k = (await cur(page, 'C.zoom()')) / 100 * 96 / 72;
      const p = await edgeOf(page, 'day-2-1');
      const dx = (midX - (b[0] + b[2]) / 2) * k - 1;   // a pixel short of the middle
      await page.mouse.move(p.x, p.y); await page.mouse.down(); await page.mouse.move(p.x + dx, p.y + 0.4, { steps: 12 }); await settle(page);
      const guides = await H(page, () => ({ all: document.querySelectorAll('.ce-ui .u-gd').length, mid: document.querySelectorAll('.ce-ui .u-gd--mid').length, read: (document.querySelector('.ce-ui .u-dim text') || {}).textContent }));
      shots.push(await shot(page, 'desk-9a-guides.png'));
      await page.mouse.up(); await settle(page);
      const b2 = await box(page, 'day-2-1');
      assert.ok(guides.all > 0 && guides.mid > 0 && /^X \d+\.\d\d {2}Y \d+\.\d\d mm$/.test(guides.read), 'the middle\'s dashed guide and the position read-out while dragging: ' + JSON.stringify(guides));
      assert.ok(near((b2[0] + b2[2]) / 2, midX, 1e-6), 'snapped onto the middle: ' + ((b2[0] + b2[2]) / 2 - midX));
      await page.keyboard.press('Control+z'); await settle(page);
      const p2 = await edgeOf(page, 'day-2-1');
      await drag(page, p2, { x: p2.x + dx, y: p2.y + 0.4 }, { keys: ['Alt'] });
      const b3 = await box(page, 'day-2-1');
      assert.ok(!near((b3[0] + b3[2]) / 2, midX, 0.5 / k), 'Alt: no snap');
    });
    await step('2.10b a side handle dragged to a pixel short of the next square\'s edge lands on it exactly (resizing snaps too)', async () => {
      await page.keyboard.press('Escape');
      await clickAt(page, await edgeOf(page, 'day-3-0'));
      const b = await box(page, 'day-3-0'), n = await box(page, 'day-3-1'), k = (await cur(page, 'C.zoom()')) / 100 * 96 / 72;
      const e = await handle(page, 'e');
      await drag(page, e, { x: e.x + (n[0] - b[2]) * k - 1, y: e.y });
      const b2 = await box(page, 'day-3-0');
      assert.ok(near(b2[2], n[0], 1e-6) && near(b2[0], b[0], 1e-6), 'right edge on the neighbour\'s left edge: ' + (b2[2] - n[0]));
      await page.keyboard.press('Control+z'); await settle(page);
    });
    await step('2.11 the back: words double-clicked open in place, new words keep their anchor; a click takes the Stars group whole, Alt-click one star', async () => {
      await page.locator('.ce-root .ce-seg [data-face="back"]').click(); await settle(page);
      assert.equal(await cur(page, 'C.face()'), 'back');
      assert.ok(await H(page, () => document.querySelector('.ce-dock__tab[data-face="back"]').classList.contains('is-on')), 'the Layers tab follows');
      const w0 = await H(page, () => window.CharmEditor.current.items().find(i => i.id === 'b-words'));
      const b = await box(page, 'b-words'), c = await ptClient(page, (b[0] + b[2]) / 2, (b[1] + b[3]) / 2);
      await page.mouse.dblclick(c.x, c.y); await settle(page);
      assert.equal(await H(page, () => document.querySelector('.ce-editor') && document.querySelector('.ce-editor').value), 'FOREVER');
      await page.keyboard.press('Control+a'); await page.keyboard.type('ALWAYS'); await page.keyboard.press('Enter'); await settle(page);
      const w1 = await H(page, () => window.CharmEditor.current.items().find(i => i.id === 'b-words'));
      assert.equal(w1.text, 'ALWAYS'); assert.ok(near(w1.tx, w0.tx, 1e-6) && near(w1.ty, w0.ty, 1e-6), 'anchor kept');
      const b2 = await box(page, 'b-words');
      assert.ok(near((b2[0] + b2[2]) / 2, (b[0] + b[2]) / 2, 0.6), 'still centred where it was');
      const s = await box(page, 'b-star-1'), sp = await ptClient(page, (s[0] + s[2]) / 2, s[1] + 0.05);
      await clickAt(page, sp);
      assert.deepEqual(await cur(page, 'C.selection()'), ['b-stars']);
      await clickAt(page, sp, { modifiers: ['Alt'] });
      assert.deepEqual(await cur(page, 'C.selection()'), ['b-star-1']);
      const r = await cur(page, 'C.result()');
      assert.deepEqual(Object.keys(r.changes.back.changed), ['b-words']);
      assert.ok(r.changes.back.changed['b-words'].subpaths.length > 0, 'outlined');
      shots.push(await shot(page, 'desk-9-back.png'));
    });
    await step('2.12 Cancel with changes asks first: Keep editing keeps them; Esc with nothing selected asks again; Discard resolves null', async () => {
      await page.keyboard.press('Escape');
      await page.locator('.ce-root .ce-foot [data-act="cancel"]').click(); await settle(page);
      assert.ok(await H(page, () => !!document.querySelector('.ce-ask')), 'asked');
      shots.push(await shot(page, 'desk-10-ask.png'));
      await page.locator('.ce-ask [data-ask="no"]').click(); await settle(page);
      assert.ok(await H(page, () => !document.querySelector('.ce-ask') && !!document.querySelector('.ce-root') && window.__closed === false));
      await page.keyboard.press('Escape'); await settle(page);
      assert.ok(await H(page, () => !!document.querySelector('.ce-ask')), 'Esc asks too');
      await page.locator('.ce-ask [data-ask="yes"]').click();
      await page.waitForFunction(() => window.__closed === true);
      assert.equal(await H(page, () => window.__result), null);
      assert.deepEqual(page.__errs, []);
      assert.equal(await H(page, () => window.__pageKeys), 0);
    });
    await page.context().close();
  }

  /* ════ 3 · phone width ════ */
  {
    const page = await newPage(390, 844);
    await openIt(page);
    await step('3.1 at 390 × 844 nothing overlaps or is cut off; the Layers start folded away; the charm fills the room', async () => {
      assert.deepEqual(await H(page, layoutIssues), []);
      assert.ok(await H(page, () => document.querySelector('.ce-dock').hidden));
      const r = await H(page, () => { const p = document.querySelector('.ce-paper').getBoundingClientRect(), s = document.querySelector('.ce-stage').getBoundingClientRect(); return [p.width / s.width, p.height / s.height]; });
      assert.ok(Math.max(r[0], r[1]) > 0.7, 'the charm fills the room: ' + r);
      shots.push(await shot(page, 'phone-1-open.png'));
    });
    await step('3.2 with something selected the bar wraps, still with nothing overlapping', async () => {
      await clickAt(page, await edgeOf(page, 'day-1-2'));
      assert.deepEqual(await cur(page, 'C.selection()'), ['day-1-2']);
      assert.deepEqual(await H(page, layoutIssues), []);
      shots.push(await shot(page, 'phone-2-selected.png'));
      await page.keyboard.press('t');
      const b = await box(page, 'day-1-2');
      await clickAt(page, await ptClient(page, b[0], b[3] + 4));
      await page.keyboard.type('HI'); await settle(page);
      assert.deepEqual(await H(page, layoutIssues), [], 'with the words box open');
      shots.push(await shot(page, 'phone-3-typing.png'));
      await page.keyboard.press('Enter'); await settle(page);
      assert.deepEqual(await H(page, layoutIssues), [], 'with a text selected (font, size, role)');
    });
    await step('3.3 the Layers fold over the room on a phone and fold away again', async () => {
      await page.locator('.ce-root .ce-head [data-act="dock"]').click(); await settle(page);
      assert.ok(await H(page, () => !document.querySelector('.ce-dock').hidden));
      assert.deepEqual(await H(page, layoutIssues), []);
      shots.push(await shot(page, 'phone-4-layers.png'));
      await page.locator('.ce-root .ce-dock [data-act="dock"]').click(); await settle(page);
      assert.ok(await H(page, () => document.querySelector('.ce-dock').hidden));
    });
    await step('3.4 the close button asks (there is a change) and Discard leaves with null', async () => {
      await page.locator('.ce-root .ce-close').click(); await settle(page);
      await page.locator('.ce-ask [data-ask="yes"]').click();
      await page.waitForFunction(() => window.__closed === true);
      assert.equal(await H(page, () => window.__result), null);
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }

  /* ════ 4 · the app's font engine, lockRoles ════ */
  {
    const page = await newPage(1280, 720);
    await page.evaluate(() => {
      window.__engine = [];
      window.openEditor({ startFace: 'back', lockRoles: [], textToPaths: (t, f, s, o) => { window.__engine.push([t, f, s, o && o.align]); return window.CharmEditor.builtinTextToPaths(t, f, s, o); } });
    });
    await page.waitForSelector('.ce-root .ce-items path'); await settle(page);
    await step('4.1 the font engine given is the one used (with the words, font key, size in pt and alignment); lockRoles [] leaves the cut line free', async () => {
      const calls = await H(page, () => window.__engine);
      assert.ok(calls.some(c => c[0] === 'FOREVER' && c[1] === 'sans' && c[2] === 10 && c[3] === 'center'), JSON.stringify(calls));
      assert.ok(await H(page, () => !window.CharmEditor.current.items().some(i => i.locked)));
      const b = await box(page, 'b-cut-outline');
      await clickAt(page, await ptClient(page, b[0], (b[1] + b[3]) / 2));
      assert.deepEqual(await cur(page, 'C.selection()'), ['b-cut-outline']);
    });
    await step('4.2 a second open while one is open is refused; Save with nothing done changes nothing', async () => {
      const err = await H(page, () => window.CharmEditor.open({}).then(() => 'opened', e => e.message));
      assert.match(err, /already open/);
      await page.keyboard.press('Control+s');
      await page.waitForFunction(() => window.__closed === true);
      const r = await H(page, () => window.__result);
      for (const f of ['front', 'back']) assert.deepEqual([r.changes[f].removed.length, r.changes[f].added.length, Object.keys(r.changes[f].changed).length], [0, 0, 0], f);
      assert.ok(r.faces.back.items.find(i => i.id === 'b-words').subpaths.length > 0);
    });
    await page.context().close();
  }

  /* ════ 5 · the busiest bar at 1280 × 720 ════ */
  {
    const page = await newPage(1280, 720);
    await openIt(page, { startFace: 'back' });
    await step('5.1 with words selected (position, size, angle, font, size, role) nothing overlaps at 1280 × 720 and the room keeps most of the window', async () => {
      const b = await box(page, 'b-words');
      await clickAt(page, await ptClient(page, (b[0] + b[2]) / 2, (b[1] + b[3]) / 2));
      assert.deepEqual(await cur(page, 'C.selection()'), ['b-words']);
      assert.ok(await H(page, () => !document.querySelector('[data-drop="font"]').hidden && !document.querySelector('[data-drop="role"]').hidden));
      assert.deepEqual(await H(page, layoutIssues), []);
      const w = await H(page, () => document.querySelector('.ce-stage').getBoundingClientRect());
      assert.ok(w.width >= 1280 * 0.6 && w.height >= 720 * 0.6, 'room ' + w.width + ' × ' + w.height);
      shots.push(await shot(page, 'desk-11-text-selected.png'));
      await page.locator('.ce-root [data-drop="role"] .ce-drop__b').click(); await settle(page);
      assert.deepEqual(await H(page, layoutIssues), []);
      shots.push(await shot(page, 'desk-12-role-menu.png'));
      await page.locator('.ce-root [data-role="line"]').click(); await settle(page);
      assert.equal(await H(page, () => window.CharmEditor.current.items().find(i => i.id === 'b-words').role), 'line');
      await page.keyboard.press('Escape');
      await page.locator('.ce-root [data-drop="more"] .ce-drop__b').click(); await settle(page);
      shots.push(await shot(page, 'desk-13-more.png'));
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }

  /* ════ 6 · weight and line width (Paul, 27 Oct 2026) ════ */
  {
    const page = await newPage(1280, 720);
    await openIt(page, { startFace: 'back' });
    const PT = 72 / 25.4;
    const one = id => H(page, id => { let f = null; const walk = l => l.forEach(i => { if (i.id === id) f = i; if (i.children) walk(i.children); }); walk(window.CharmEditor.current.items()); return f; }, id);
    const inkOf = id => H(page, id => { const r = window.CharmEditor.current.result(); let f = null; const walk = l => l.forEach(i => { if (i.id === id) f = i; if (i.children) walk(i.children); }); walk(r.faces.back.items); return window.CharmEditor.model.inkArea(f.subpaths); }, id);
    const val = f => page.locator(`.ce-root [data-f="${f}"]`).inputValue();
    const shown = w => H(page, w => !document.querySelector(`.ce-root [data-when="${w}"]`).hidden, w);
    const rows = () => H(page, () => new Set([...document.querySelector('.ce-props').children].filter(e => e.getBoundingClientRect().height > 0).map(e => Math.round(e.getBoundingClientRect().top))).size);
    const undoN = async () => (await cur(page, 'C.history()')).undo;
    const centre = b => [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2];
    let b0 = null, ink0 = 0;
    await step('6.1 words selected: a Weight field (its icon, 0.00 mm, the font\'s own) beside the size; the bar is ONE row at 1280 × 720 and nothing overlaps; no Stroke for filled words', async () => {
      const b = await box(page, 'b-words');
      await clickAt(page, await ptClient(page, (b[0] + b[2]) / 2, (b[1] + b[3]) / 2));
      assert.deepEqual(await cur(page, 'C.selection()'), ['b-words']);
      assert.ok(await shown('weight') && !(await shown('stroke')), 'Weight shown, Stroke not');
      assert.equal(await val('wt'), '0.00');
      assert.ok(await H(page, () => !!document.querySelector('.ce-root [data-act="wt0"] svg') && /mm/.test(document.querySelector('.ce-root [data-when="weight"]').textContent)), 'an icon and mm');
      assert.equal(await rows(), 1);
      assert.deepEqual(await H(page, layoutIssues), []);
      b0 = await box(page, 'b-words'); ink0 = await inkOf('b-words');
      shots.push(await shot(page, 'weight-1-bar.png'));
    });
    await step('6.2 typing +0.05: every stroke of the words 0.05 mm thicker (weightMm, more ink, each edge out), drawn where they were, ONE undo step; the result carries the weight and those outlines', async () => {
      const u0 = await undoN();
      await typeField(page, 'wt', '0.05');
      const it = await one('b-words'), b = await box(page, 'b-words'), ink = await inkOf('b-words');
      assert.equal(it.weightMm, 0.05); assert.equal(await val('wt'), '+0.05');
      assert.ok(ink > ink0 * 1.05, 'more ink: ' + ink0 + ' -> ' + ink);
      assert.ok(b[0] < b0[0] - 0.02 * PT && b[2] > b0[2] + 0.02 * PT && b[1] < b0[1] - 0.02 * PT && b[3] > b0[3] + 0.02 * PT, 'each edge out by 0.025 mm or more: ' + JSON.stringify([b0, b]));
      assert.ok(near(centre(b)[0], centre(b0)[0], 0.01 * PT) && near(centre(b)[1], centre(b0)[1], 0.01 * PT), 'where they were');
      assert.equal(await undoN(), u0 + 1);
      const r = await cur(page, 'C.result()'), o = r.faces.back.items.find(i => i.id === 'b-words');
      assert.ok(o.weightMm === 0.05 && Object.keys(r.changes.back.changed).join() === 'b-words' && o.bbox.every((v, i) => near(v, b[i], 1e-6)), 'the result: the weight, and the outlines as drawn');
      assert.deepEqual(await cur(page, 'C.selection()'), ['b-words'], 'still selected');
      shots.push(await shot(page, 'weight-2-plus005.png'));
    });
    await step('6.3 the arrow keys step it by 0.01 mm: ↑ ↑ ↑ then ↓ is +0.07, the outlines grow with every tap and shrink with the ↓, and the run is ONE undo step', async () => {
      const u0 = await undoN(), inks = [await inkOf('b-words')];
      await page.locator('.ce-root [data-f="wt"]').click();
      for (const k of ['ArrowUp', 'ArrowUp', 'ArrowUp', 'ArrowDown']) { await page.keyboard.press(k); await settle(page); inks.push(await inkOf('b-words')); }
      await page.keyboard.press('Enter'); await settle(page);
      assert.equal((await one('b-words')).weightMm, 0.07); assert.equal(await val('wt'), '+0.07');
      assert.ok(inks[0] < inks[1] && inks[1] < inks[2] && inks[2] < inks[3] && inks[4] < inks[3] && near(inks[4], inks[2], 1e-6), 'monotonic with the weight: ' + inks.map(v => v.toFixed(3)).join(' '));
      assert.equal(await undoN(), u0 + 1, 'four taps, one step');
      await page.locator('.ce-root [data-f="wt"]').click(); await page.keyboard.press('Shift+ArrowDown'); await page.keyboard.press('Enter'); await settle(page);
      assert.equal((await one('b-words')).weightMm, 0.02, 'Shift steps 0.05 mm');
    });
    await step('6.4 thinner than the font: -0.03 mm is less ink than the font\'s own; the icon puts the font\'s own weight back, and then there is nothing to save', async () => {
      await typeField(page, 'wt', '-0.03');
      const it = await one('b-words'), ink = await inkOf('b-words'), b = await box(page, 'b-words');
      assert.ok(it.weightMm === -0.03 && ink < ink0 * 0.95 && b[2] - b[0] < b0[2] - b0[0], 'thinner: ' + ink0 + ' -> ' + ink);
      assert.equal(await val('wt'), '-0.03');
      assert.ok(await H(page, () => document.querySelector('.ce-root [data-act="wt0"]').classList.contains('is-on')), 'the icon says the weight is not the font\'s own');
      shots.push(await shot(page, 'weight-3-minus003.png'));
      await page.locator('.ce-root [data-act="wt0"]').click(); await settle(page);
      const back = await one('b-words');
      assert.ok(!('weightMm' in back) && await val('wt') === '0.00' && near(await inkOf('b-words'), ink0, 1e-9), 'the font\'s own again');
      assert.equal(Object.keys((await cur(page, 'C.result()')).changes.back.changed).length, 0, 'no change left');
    });
    await step('6.5 undo and redo walk the weights back and forth (-0.03, +0.02, +0.07, +0.05, the font\'s own)', async () => {
      const ws = [];
      for (let i = 0; i < 5; i++) { await page.keyboard.press('Control+z'); await settle(page); const it = await one('b-words'); ws.push(it.weightMm == null ? 0 : it.weightMm); }
      assert.deepEqual(ws, [-0.03, 0.02, 0.07, 0.05, 0]);
      await page.keyboard.press('Control+Shift+z'); await settle(page);
      assert.equal((await one('b-words')).weightMm, 0.05);
      assert.equal(await val('wt'), '+0.05', 'the field follows');
    });
    await step('6.6 Line engrave: the words take a Stroke width instead of a weight (0.088 mm: the 0.25 pt the laser file writes), drawn at exactly that; 0.03 mm thins them and the result says it', async () => {
      await page.locator('.ce-root [data-drop="role"] .ce-drop__b').click(); await page.locator('.ce-root [data-role="line"]').click(); await settle(page);
      assert.ok(await shown('stroke') && !(await shown('weight')), 'Stroke shown, Weight not');
      assert.equal(await val('lw'), '0.088');
      assert.equal(await rows(), 1, 'still one row');
      await page.evaluate(() => window.CharmEditor.current.setZoom(1000)); await settle(page);
      const w = () => H(page, () => parseFloat(getComputedStyle(document.querySelector('.ce-items path[data-id="b-words"]')).strokeWidth));
      assert.ok(near(await w(), 0.25 * 10 * 96 / 72, 1e-3), 'at 1000 %: 0.25 pt × 13.33 px = ' + await w());
      await typeField(page, 'lw', '0.03');
      const it = await one('b-words');
      assert.ok(near(it.lwPt, 0.03 * PT, 1e-6) && near(await w(), 0.03 * PT * 10 * 96 / 72, 1e-3), 'thinned: ' + it.lwPt + ' pt, ' + await w() + ' px');
      const o = (await cur(page, 'C.result()')).faces.back.items.find(i => i.id === 'b-words');
      assert.ok(near(o.lwPt, 0.03 * PT, 1e-6) && o.role === 'line');
      await page.evaluate(() => window.CharmEditor.current.setZoom(100)); await settle(page);
      assert.ok(near(await w(), 1, 1e-6), 'at 100 % a line this thin is held at one screen pixel (the hairline floor), no more: ' + await w());
      await page.keyboard.press('Control+0'); await settle(page);
      shots.push(await shot(page, 'weight-4-line-stroke.png'));
    });
    await step('6.7 the stars (a group of red lines) read their 0.088 mm; ↓ ↓ ↓ ↓ thins both to 0.048 mm in ONE undo step; the icon gives each its own width back', async () => {
      await page.keyboard.press('Escape');
      const b = await box(page, 'b-star-1');
      await clickAt(page, await ptClient(page, (b[0] + b[2]) / 2, b[1] + 0.02));
      const sel = await cur(page, 'C.selection()');
      assert.ok(sel.length === 1 && (await one(sel[0])).kind === 'group', 'the group: ' + sel);
      assert.equal(await val('lw'), '0.088');
      const u0 = await undoN();
      await page.locator('.ce-root [data-f="lw"]').click();
      for (let i = 0; i < 4; i++) await page.keyboard.press('ArrowDown');
      await page.keyboard.press('Enter'); await settle(page);
      const g = await one(sel[0]);
      assert.ok(g.children.every(c => near(c.lwPt, 0.048 * PT, 1e-6)) && await undoN() === u0 + 1, JSON.stringify(g.children.map(c => c.lwPt)));
      await page.locator('.ce-root [data-act="lw0"]').click(); await settle(page);
      assert.ok((await one(sel[0])).children.every(c => c.lwPt === 0.25), 'their own 0.25 pt again');
    });
    await step('6.8 the Text tool with nothing selected arms the weight of the next words: +0.03, then a click and "HI" are made at +0.03', async () => {
      await page.keyboard.press('Escape'); await page.keyboard.press('t'); await settle(page);
      assert.ok(await shown('weight'), 'the Weight field is there before the words');
      await typeField(page, 'wt', '0.03');
      const f = await H(page, () => window.CharmEditor.current.frame());
      await clickAt(page, await ptClient(page, f.x0 + 4 * PT, f.y0 + 26 * PT));
      await page.keyboard.type('HI'); await page.keyboard.press('Enter'); await settle(page);
      const hi = await findText(page, 'HI');
      assert.ok(hi && hi.weightMm === 0.03, JSON.stringify(hi));
      assert.equal(await val('wt'), '+0.03');
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }
  /* a line at its true width at every zoom, counted in the screen's own pixels */
  {
    const page = await newPage(1280, 720);
    await openIt(page);
    const PT = 72 / 25.4;
    // the dark pixels across a day square's left line, on one row of the screen (the square's grey ink on white), partial pixels counted by their darkness: the grid off, nothing
    // selected (no selection box over the line), and only 0.12 mm either side of it (the next square's line is 0.32 mm away)
    const across = async id => {
      await page.mouse.move(4, 360); await settle(page);   // (the hand off the room: no hover outline over the line)
      const b = await box(page, id), p = await ptClient(page, b[0], (b[1] + b[3]) / 2), k = await cur(page, 'C.zoom()') / 100 * 96 / 72, hw = Math.max(3, Math.floor(0.12 * PT * k));
      const buf = await page.screenshot({ clip: { x: Math.round(p.x) - hw, y: Math.round(p.y) + 3, width: 2 * hw, height: 1 } });
      return H(page, async b64 => {
        const img = new Image(); img.src = 'data:image/png;base64,' + b64; await img.decode();
        const c = document.createElement('canvas'); c.width = img.width; c.height = img.height; const g = c.getContext('2d'); g.drawImage(img, 0, 0);
        const d = g.getImageData(0, 0, img.width, 1).data; let n = 0;
        for (let i = 0; i < d.length; i += 4) { const l = 0.299 * d[i] + 0.587 * d[i + 1] + 0.114 * d[i + 2]; n += Math.max(0, Math.min(1, (235 - l) / (235 - 84))); }
        return n;
      }, buf.toString('base64'));
    };
    await step('6.9 a line is drawn at its TRUE width at every zoom: a day square\'s 0.3 pt line is 8 px across at 2000 %, 4 px at 1000 %; thinned to 0.03 mm it is 2.3 px at 2000 %; at 100 % only a hairline', async () => {
      await H(page, () => document.querySelector('.ce-root [data-opt="grid"]').click()); await settle(page);   // (the More menu's "Millimetre grid", off)
      await clickAt(page, await edgeOf(page, 'day-2-3'));
      await page.keyboard.press('Control+2'); await settle(page);
      const zoomTo = async z => { await page.evaluate(z => window.CharmEditor.current.setZoom(z), z); await settle(page); };
      await page.keyboard.press('Escape'); await settle(page);
      await zoomTo(2000); const at2000 = await across('day-2-3');
      await zoomTo(1000); const at1000 = await across('day-2-3');
      assert.ok(near(at2000, 0.3 * 20 * 96 / 72, 1) && near(at1000, 0.3 * 10 * 96 / 72, 1), 'dark pixels across: ' + at2000.toFixed(2) + ' at 2000 %, ' + at1000.toFixed(2) + ' at 1000 % (true: 8, 4)');
      await zoomTo(2000);
      await clickAt(page, await edgeOf(page, 'day-2-3', 0.2));
      assert.deepEqual(await cur(page, 'C.selection()'), ['day-2-3']);
      assert.equal(await page.locator('.ce-root [data-f="lw"]').inputValue(), '0.106', 'the Stroke field reads it: 0.3 pt = 0.106 mm');
      await typeField(page, 'lw', '0.03');
      await page.keyboard.press('Escape'); await settle(page);
      const thin = await across('day-2-3');
      shots.push(await shot(page, 'weight-5-thin-line-2000.png'));
      assert.ok(near(thin, 0.03 * PT * 20 * 96 / 72, 1), 'thinned: ' + thin.toFixed(2) + ' px (true: ' + (0.03 * PT * 20 * 96 / 72).toFixed(2) + ')');
      await zoomTo(100);
      const w = await H(page, () => parseFloat(getComputedStyle(document.querySelector('.ce-items path[data-id="day-2-3"]')).strokeWidth));
      assert.ok(near(w, 1, 1e-6), 'at 100 % the hairline floor: ' + w);
      assert.deepEqual(page.__errs, []);
    });
    await page.context().close();
  }

  await browser.close();
  if (SHOTS) console.log('  screenshots: ' + shots.filter(Boolean).join(', '));
  console.log(`\n${results.length} passed, ${failures.length} failed`);
  if (failures.length) { console.log(failures.map(f => '  - ' + f).join('\n')); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
