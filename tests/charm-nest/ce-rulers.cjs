// RULERS, GUIDES, ZOOM AND PAN IN THE CHARM EDITOR (charm-nest-charm-editor.js; Paul, 28 Oct 2026: "I also don't see a ruler like we have already in the charm studio
// ... I also need the ability to zoom into a given feature in the vector file. I need the ability to pan the work area once zoomed in"). One real Chromium page with
// nothing but the editor (fixtures/charm-editor-harness.html); no server, no network. Everything is done with the mouse, the wheel and the keys, and the rulers are
// read off their own canvas pixels.
//   PW_DIR=/opt/node-tools/node_modules CHROMIUM=/opt/pw-browsers/chromium-1243/chrome-linux64/chrome node tests/charm-nest/ce-rulers.cjs     (SHOTS=<dir> keeps screenshots)
// 1 the rulers: the selection's extent marked on both and following it as it moves; the cursor on both; ticks that refine with the zoom (0.1 mm and finer when
//   zoomed in), counted on the canvas
// 2 guides: dragged out of the top ruler onto a day square's edge (it settles on it exactly), a part dragged to it snaps to it, moved, Shift on the ruler's ticks,
//   the other face has its own, dragged back onto the ruler it is gone; the zero point dragged from the corner onto a part's corner: X / Y read 0, the rulers'
//   zero is there; a double-click on the corner puts it back
// 3 zoom: the Zoom tool (Z) clicks in up Illustrator's ladder about the cursor, Alt-click out, a box swept fills the view, a double-click on a part fills the view
//   with it; ⌘2 zooms to the selection; ⌘ + / − / 0 / 1; the zoom box typed (250 %) and its 6400 % preset (still SVG, the viewBox carries it); a trackpad pinch
// 4 pan: Space-drag, the Hand (H), the middle button and a trackpad's two-finger scroll each move the view by exactly the hand's travel
// 5 frame work on a 300-path charm: the main-thread time of each frame of a pan and of a wheel zoom (CDP task time, not a wall clock), nothing selected and all
//   300 selected; every frame under 16 ms of work
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const pwDir = process.env.PW_DIR || path.join(__dirname, '../../node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const HARNESS = 'file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html');
const PT = 72 / 25.4, MM = 25.4 / 72, PX100 = 96 / 72;
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 10).join('\n         ')); };
async function step(name, fn) { try { const note = await fn(); pass(name + (note ? '  [' + note + ']' : '')); } catch (e) { fail(name, e); } }
const near = (a, b, eps) => Math.abs(a - b) <= eps;

const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const C = page => page.evaluate(() => { const c = window.CharmEditor.current; return { zoom: c.zoom(), sel: c.selection(), tool: c.tool(), zero: c.zero(), guides: c.guides(), frame: c.frame() }; });
const clientOf = (page, x, y) => page.evaluate(([x, y]) => window.CharmEditor.current.clientOf(x, y), [x, y]);
const ptOf = (page, cx, cy) => page.evaluate(([x, y]) => window.CharmEditor.current.ptOf(x, y), [cx, cy]);
const box = (page, id) => page.evaluate(id => window.CharmEditor.current.box(id), id);
const rect = (page, sel) => page.evaluate(s => { const r = document.querySelector(s).getBoundingClientRect(); return { l: r.left, t: r.top, w: r.width, h: r.height, r: r.right, b: r.bottom }; }, sel);
async function drag(page, a, b, o) {
  o = o || {};
  for (const k of o.keys || []) await page.keyboard.down(k);
  await page.mouse.move(a.x, a.y); await page.mouse.down(o.button ? { button: o.button } : undefined);
  await page.mouse.move(b.x, b.y, { steps: o.steps || 10 }); await page.mouse.up(o.button ? { button: o.button } : undefined);
  for (const k of (o.keys || []).slice().reverse()) await page.keyboard.up(k);
  await settle(page);
}
async function clickAt(page, p, mods) {
  for (const k of mods || []) await page.keyboard.down(k);
  await page.mouse.click(p.x, p.y);
  for (const k of (mods || []).slice().reverse()) await page.keyboard.up(k);
  await settle(page);
}
/** One pixel of a ruler, at ruler-px (x along it, y across it). */
const rulerPx = (page, which, along, across) => page.evaluate(([w, a, c]) => {
  const cv = document.querySelector(w === 'x' ? '.ce-rule--x' : '.ce-rule--y'), dpr = cv.width / cv.clientWidth;
  const x = w === 'x' ? a : c, y = w === 'x' ? c : a;
  return Array.from(cv.getContext('2d').getImageData(Math.floor(x * dpr), Math.floor(y * dpr), 1, 1).data);
}, [which, along, across]);
const isGold = px => px[3] > 200 && near(px[0], 184, 30) && near(px[1], 147, 30) && near(px[2], 90, 35);
/** The ticks along the top ruler's inner edge: their positions (ruler px) and the median gap. */
const tickGap = page => page.evaluate(() => {
  const cv = document.querySelector('.ce-rule--x'), w = cv.width, h = cv.height, dpr = w / cv.clientWidth;
  const row = cv.getContext('2d').getImageData(0, h - Math.ceil(2 * dpr), w, 1).data, at = [];
  for (let x = 0; x < w; x++) if (row[x * 4 + 3] > 40) { if (!at.length || x - at[at.length - 1] > 1) at.push(x); }
  const gaps = at.slice(1).map((x, i) => (x - at[i]) / dpr).sort((a, b) => a - b);
  return { ticks: at.length, gap: gaps.length ? gaps[Math.floor(gaps.length / 2)] : 0 };
});

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
  const openIt = async (page, extra) => { await page.evaluate(x => { window.openEditor(x || undefined); }, extra || null); await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page); };
  const shot = async (page, name) => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name) }); };

  const page = await newPage(1280, 720);
  await openIt(page);
  const stageR = await rect(page, '.ce-stage'), rxR = await rect(page, '.ce-rule--x'), ryR = await rect(page, '.ce-rule--y'), cornerR = await rect(page, '.ce-rule__corner');
  const away = async () => { await page.mouse.move(stageR.l + stageR.w / 2, stageR.b + 40); await settle(page); };   // (off the room: no cursor line on the rulers)
  const edge = async (id, at) => { const b = await box(page, id); return clientOf(page, b[0], b[1] + (b[3] - b[1]) * (at == null ? 0.5 : at)); };

  /* ════ 1 · the rulers ════ */
  await step('1.1 the selection\'s extent is marked on both rulers (its edges in gold), and the marks follow it as it moves', async () => {
    await clickAt(page, await edge('day-2-3', 0.3)); await away();
    assert.deepEqual((await C(page)).sel, ['day-2-3']);
    const marks = async () => { const b = await box(page, 'day-2-3'), a = await clientOf(page, b[0], b[1]), c = await clientOf(page, b[2], b[3]); return { x0: Math.round(a.x - rxR.l), x1: Math.round(c.x - rxR.l), y0: Math.round(a.y - ryR.t), y1: Math.round(c.y - ryR.t) }; };
    const m0 = await marks();
    for (const [w, p] of [['x', m0.x0], ['x', m0.x1], ['y', m0.y0], ['y', m0.y1]]) assert.ok(isGold(await rulerPx(page, w, p, 4)), `gold at ${w} ${p}: ${await rulerPx(page, w, p, 4)}`);
    assert.ok(!isGold(await rulerPx(page, 'x', m0.x0 - 6, 4)), 'no mark beside it');
    await page.keyboard.press('Shift+ArrowRight'); await page.keyboard.press('Shift+ArrowDown'); await settle(page);   // 1 mm right, 1 mm down
    const m1 = await marks();
    assert.ok(near(m1.x0 - m0.x0, PT * (await C(page)).zoom / 100 * PX100, 1.01), 'moved a millimetre: ' + (m1.x0 - m0.x0));
    for (const [w, p] of [['x', m1.x0], ['x', m1.x1], ['y', m1.y0], ['y', m1.y1]]) assert.ok(isGold(await rulerPx(page, w, p, 4)), `gold at ${w} ${p} after the move`);
    assert.ok(!isGold(await rulerPx(page, 'x', m0.x0, 4)) || m1.x0 - m0.x0 < 2, 'the old mark is gone');
    await page.keyboard.press('Control+z'); await settle(page);
    await shot(page, 'r1-marks.png');
    return `marks x ${m0.x0}–${m0.x1}, y ${m0.y0}–${m0.y1} px; after 1 mm: x ${m1.x0}`;
  });
  await step('1.2 the cursor is tracked on both rulers', async () => {
    await page.keyboard.press('Escape'); await settle(page);
    const p = { x: Math.round(stageR.l + stageR.w * 0.31), y: Math.round(stageR.t + stageR.h * 0.63) };
    await page.mouse.move(p.x, p.y); await settle(page);
    assert.ok(isGold(await rulerPx(page, 'x', p.x - rxR.l, 3)), 'top ruler: ' + await rulerPx(page, 'x', p.x - rxR.l, 3));
    assert.ok(isGold(await rulerPx(page, 'y', p.y - ryR.t, 3)), 'left ruler');
    await away();
    assert.ok(!isGold(await rulerPx(page, 'x', p.x - rxR.l, 3)), 'gone when the hand leaves the room');
  });
  await step('1.3 the ticks refine with the zoom: 2 mm at 100 %, 0.5 mm at 400 %, 0.1 mm at 2000 %, 0.05 mm at 6400 % (gaps counted on the canvas)', async () => {
    const out = [];
    for (const [z, mm] of [[100, 2], [400, 0.5], [2000, 0.1], [6400, 0.05]]) {
      await page.evaluate(z => window.CharmEditor.current.setZoom(z), z); await settle(page);
      const t = await tickGap(page), pxmm = z / 100 * PX100 * PT;
      out.push(`${z}%: ${t.ticks} ticks ${(t.gap / pxmm).toFixed(3)} mm apart`);
      assert.ok(near(t.gap / pxmm, mm, mm * 0.2), `${z} %: a tick every ${(t.gap / pxmm).toFixed(3)} mm, want ${mm}`);
    }
    await shot(page, 'r1-6400.png');
    await page.keyboard.press('Control+0'); await settle(page);
    return out.join(' · ');
  });

  /* ════ 2 · guides and the zero point ════ */
  await step('2.1 a guide dragged out of the top ruler settles exactly on a day square\'s top edge (dropped 3 px off it), and shows on the ruler', async () => {
    const b = await box(page, 'day-1-1'), top = await clientOf(page, b[0], b[1]);
    const from = { x: rxR.l + rxR.w * 0.6, y: rxR.t + rxR.h / 2 };
    await drag(page, from, { x: from.x, y: top.y + 3 });
    const g = (await C(page)).guides;
    assert.equal(g.length, 1); assert.equal(g[0].axis, 'y');
    assert.ok(near(g[0].v, b[1], 1e-6), `guide at ${g[0].v}, edge at ${b[1]}`);
    const lines = await page.evaluate(() => document.querySelectorAll('.ce-view .v-guide').length);
    assert.equal(lines, 1);
    await away();
    return `y ${((g[0].v - (await C(page)).frame.y0) * MM).toFixed(3)} mm`;
  });
  await step('2.2 a part dragged to within a few px of the guide snaps onto it (its top edge exactly on the guide), with the smart guides off: the guide alone does it', async () => {
    const more = async () => { await page.locator('.ce-root .ce-rail [data-drop="more"] .ce-drop__b').click(); await page.locator('.ce-root [data-opt="smart"]').click(); await page.keyboard.press('Escape'); await settle(page); };
    await more();
    assert.equal(await page.evaluate(() => document.querySelector('.ce-root [data-opt="smart"]').getAttribute('aria-checked')), 'false');
    const g = (await C(page)).guides[0];
    const b = await box(page, 'heart-fill'), c = await clientOf(page, (b[0] + b[2]) / 2, (b[1] + b[3]) / 2), k = (await C(page)).zoom / 100 * PX100;
    // the heart's top goes to 4 px under the guide: the snap pulls it the rest of the way
    const dy = (g.v - b[1]) * k + 4;
    await drag(page, c, { x: c.x - 12, y: c.y + dy }, { steps: 12 });
    const b2 = await box(page, 'heart-fill');
    assert.deepEqual((await C(page)).sel, ['heart-fill']);
    assert.ok(near(b2[1], g.v, 1e-6), `heart top ${b2[1]}, guide ${g.v}`);
    await page.keyboard.press('Control+z'); await page.keyboard.press('Escape'); await settle(page);
    await more();
  });
  await step('2.3 a guide is moved by dragging it (where no part is under it); Shift puts it on the ruler\'s ticks; the other face has its own', async () => {
    const g = (await C(page)).guides[0], at = await clientOf(page, 0, g.v);
    const x = stageR.l + 30;   // the room's left margin, off the charm
    await page.mouse.move(x, at.y); await settle(page);
    assert.ok(await page.evaluate(() => document.querySelector('.ce-stage').classList.contains('is-guide-y')), 'the cursor says it can be moved');
    await drag(page, { x, y: at.y }, { x, y: at.y + 37 }, { keys: ['Shift'] });
    const g2 = (await C(page)).guides[0], f = (await C(page)).frame;
    assert.equal(g2.id, g.id);
    const mm = (g2.v - f.y0) * MM;   // (fitted, about 400 %: the ruler's divisions are half millimetres)
    assert.ok(g2.v > g.v, 'moved down');
    assert.ok(near(mm / 0.5, Math.round(mm / 0.5), 1e-6), `on a tick: ${mm} mm`);
    await page.locator('.ce-root .ce-seg [data-face="back"]').click(); await settle(page);
    assert.equal((await C(page)).guides.length, 0, 'the back has none');
    assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-view .v-guide').length), 0);
    await page.locator('.ce-root .ce-seg [data-face="front"]').click(); await settle(page);
    assert.equal((await C(page)).guides.length, 1, 'the front keeps its own');
    return `moved to ${mm.toFixed(2)} mm`;
  });
  await step('2.4 an upright guide from the left ruler; dragged back onto its ruler it is gone; ⌘; hides and shows them', async () => {
    const from = { x: ryR.l + ryR.w / 2, y: ryR.t + ryR.h * 0.5 };
    await drag(page, from, { x: stageR.l + 60, y: from.y });
    let gs = (await C(page)).guides; assert.equal(gs.length, 2); const up = gs.find(g => g.axis === 'x'); assert.ok(up);
    await page.keyboard.press('Control+;'); await settle(page);
    assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-view .v-guide').length), 0, 'hidden');
    await page.keyboard.press('Control+;'); await settle(page);
    assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-view .v-guide').length), 2, 'shown');
    const at = await clientOf(page, up.v, 0);
    await drag(page, { x: at.x, y: stageR.t + 20 }, { x: ryR.l + 4, y: stageR.t + 20 });
    gs = (await C(page)).guides; assert.equal(gs.length, 1, 'dragged back onto the ruler: gone'); assert.equal(gs[0].axis, 'y');
    await shot(page, 'r2-guides.png');
  });
  await step('2.5 the zero point dragged from the corner onto a day square\'s top-left (it settles there): X / Y read 0.00 for it, the rulers count from it; a double-click on the corner puts it back', async () => {
    const b = await box(page, 'day-2-2'), tl = await clientOf(page, b[0], b[1]);
    await drag(page, { x: cornerR.l + cornerR.w / 2, y: cornerR.t + cornerR.h / 2 }, { x: tl.x + 3, y: tl.y - 2 });
    const z = (await C(page)).zero;
    assert.ok(near(z[0], b[0], 1e-6) && near(z[1], b[1], 1e-6), `zero ${z} vs ${b}`);
    await clickAt(page, await edge('day-2-2', 0.4)); await away();
    const read = await page.evaluate(() => ({ x: document.querySelector('.ce-root [data-f="x"]').value, y: document.querySelector('.ce-root [data-f="y"]').value }));
    assert.deepEqual(read, { x: '0.00', y: '0.00' });
    // the zero tick (the full-depth line and its "0") sits at the square's corner on the top ruler
    assert.ok(await page.evaluate(() => document.querySelector('.ce-rule__corner').classList.contains('is-moved')));
    await page.locator('.ce-root .ce-rule__corner').dblclick(); await settle(page);
    const f = (await C(page)).frame, z2 = (await C(page)).zero;
    assert.ok(near(z2[0], f.x0, 1e-9) && near(z2[1], f.y0, 1e-9));
    const x = await page.evaluate(() => document.querySelector('.ce-root [data-f="x"]').value);
    assert.equal(x, ((b[0] - f.x0) * MM).toFixed(2));
    await page.keyboard.press('Escape'); await settle(page);
    return `X back to ${x} mm`;
  });

  /* ════ 3 · zoom ════ */
  await step('3.1 the Zoom tool (Z): a click zooms in one step of the ladder about the cursor (the point under it stays), Alt-click back out', async () => {
    await page.keyboard.press('Control+0'); await settle(page);
    await page.keyboard.press('z'); assert.equal((await C(page)).tool, 'zoom');
    assert.equal(await page.evaluate(() => getComputedStyle(document.querySelector('.ce-stage')).cursor), 'zoom-in');
    const z0 = (await C(page)).zoom, p = { x: Math.round(stageR.l + stageR.w * 0.42), y: Math.round(stageR.t + stageR.h * 0.38) };
    const pt0 = await ptOf(page, p.x, p.y);
    await clickAt(page, p);
    const z1 = (await C(page)).zoom, pt1 = await ptOf(page, p.x, p.y);
    const ladder = [25, 100 / 3, 50, 200 / 3, 100, 150, 200, 300, 400, 600, 800, 1200, 1600, 2400, 3200, 4800, 6400];
    assert.equal(z1, ladder.find(z => z > z0 * 1.004), `from ${z0} to ${z1}`);
    assert.ok(near(pt1[0], pt0[0], 0.02) && near(pt1[1], pt0[1], 0.02), 'the point under the cursor stays');
    await clickAt(page, p, ['Alt']);
    const z2 = (await C(page)).zoom;
    assert.equal(z2, ladder.filter(z => z < z1 / 1.004).pop());
    return `${Math.round(z0)}% → ${z1}% → ${z2}%`;
  });
  await step('3.2 a box swept with the Zoom tool fills the view: a day square\'s box centred and filling the room\'s shorter side', async () => {
    const b = await box(page, 'day-3-4'), a = await clientOf(page, b[0], b[1]), c = await clientOf(page, b[2], b[3]);
    await drag(page, a, c);
    const z = (await C(page)).zoom, k = z / 100 * PX100, a2 = await clientOf(page, b[0], b[1]), c2 = await clientOf(page, b[2], b[3]);
    const w = c2.x - a2.x, h = c2.y - a2.y;
    assert.ok(near(Math.max(w / stageR.w, h / stageR.h), 1, 0.02), `fills: ${w}×${h} in ${stageR.w}×${stageR.h}`);
    assert.ok(near((a2.x + c2.x) / 2, stageR.l + stageR.w / 2, 1.5) && near((a2.y + c2.y) / 2, stageR.t + stageR.h / 2, 1.5), 'centred');
    await shot(page, 'r3-zoombox.png');
    return `${Math.round(z)}%`;
  });
  await step('3.3 double-click a part with the Zoom tool: it fills the view; ⌘2 zooms to the selection', async () => {
    await page.keyboard.press('Control+0'); await settle(page);
    const p = await edge('heart-line', 0.5);
    const hb = await box(page, 'heart-line');
    const pt = await page.evaluate(([x, y]) => window.CharmEditor.current.clientOf(x, y), [hb[0], (hb[1] + hb[3]) / 2]);
    await page.mouse.dblclick(pt.x + 0.5, pt.y); await settle(page);
    const a = await clientOf(page, hb[0], hb[1]), c = await clientOf(page, hb[2], hb[3]);
    assert.ok(near(Math.max((c.x - a.x) / stageR.w, (c.y - a.y) / stageR.h), 0.8, 0.02), 'the heart fills 80 %: ' + ((c.x - a.x) / stageR.w).toFixed(3) + ' ' + ((c.y - a.y) / stageR.h).toFixed(3));
    const z1 = (await C(page)).zoom;
    await page.keyboard.press('v'); await page.keyboard.press('Control+0'); await settle(page);
    await clickAt(page, await edge('day-0-6', 0.5)); await page.keyboard.press('Control+2'); await settle(page);
    const sb = await box(page, 'day-0-6'), a2 = await clientOf(page, sb[0], sb[1]), c2 = await clientOf(page, sb[2], sb[3]);
    assert.ok(near(Math.max((c2.x - a2.x) / stageR.w, (c2.y - a2.y) / stageR.h), 0.6, 0.02) && near((a2.x + c2.x) / 2, stageR.l + stageR.w / 2, 1.5), 'the selection at 60 %, centred');
    return `part ${Math.round(z1)}%, selection ${Math.round((await C(page)).zoom)}%`;
  });
  await step('3.4 ⌘ + and ⌘ − step the ladder, ⌘0 fits, ⌘1 is 100 %; the zoom box takes a typed 250 and its 6400 % preset (still SVG: the viewBox carries the zoom)', async () => {
    await page.keyboard.press('Control+1'); await settle(page); assert.equal((await C(page)).zoom, 100);
    await page.keyboard.press('Control+='); await settle(page); assert.equal((await C(page)).zoom, 150);
    await page.keyboard.press('Control+='); await settle(page); assert.equal((await C(page)).zoom, 200);
    await page.keyboard.press('Control+-'); await settle(page); assert.equal((await C(page)).zoom, 150);
    await page.keyboard.press('Control+0'); await settle(page); const fz = (await C(page)).zoom; assert.ok(fz > 300 && fz < 500, 'fitted ' + fz);
    await page.locator('.ce-root .ce-zoomb').click(); await settle(page);
    assert.ok(await page.evaluate(() => document.activeElement && document.activeElement.hasAttribute('data-zoomtype')), 'the typed zoom has the keys');
    await page.keyboard.type('250'); await page.keyboard.press('Enter'); await settle(page);
    assert.ok(near((await C(page)).zoom, 250, 1e-9)); assert.equal(await page.evaluate(() => document.querySelector('[data-zoomread]').textContent), '250%');
    await page.keyboard.press('v');   // (the keys are the editor's again: a tool key works)
    assert.equal((await C(page)).tool, 'select');
    await page.locator('.ce-root .ce-zoomb').click(); await page.locator('.ce-root [data-zoom="6400"]').click(); await settle(page);
    const v = await page.evaluate(() => { const art = document.querySelector('.ce-art'), vb = art.viewBox.baseVal, r = art.getBoundingClientRect(); return { zoom: window.CharmEditor.current.zoom(), read: document.querySelector('[data-zoomread]').textContent, raster: document.querySelector('.ce-stage').querySelectorAll('img, canvas, image').length, ok: Math.abs(vb.width * (window.CharmEditor.current.zoom() / 100 * 96 / 72) - r.width) < 0.5 }; });
    assert.ok(near(v.zoom, 6400, 1e-9) && v.read === '6400%' && v.raster === 0 && v.ok, JSON.stringify(v));
    await page.keyboard.press('Control+='); await settle(page); assert.equal((await C(page)).zoom, 6400, 'the top');
    await page.keyboard.press('Control+0'); await settle(page);
  });
  await step('3.5 a trackpad pinch (⌘-wheel) zooms about the fingers; over a ruler too, and never the page', async () => {
    const p = { x: Math.round(stageR.l + stageR.w * 0.6), y: Math.round(stageR.t + stageR.h * 0.45) };
    await page.mouse.move(p.x, p.y);
    const z0 = (await C(page)).zoom, pt0 = await ptOf(page, p.x, p.y);
    await page.keyboard.down('Control'); for (let i = 0; i < 6; i++) await page.mouse.wheel(0, -8); await page.keyboard.up('Control'); await settle(page);
    const z1 = (await C(page)).zoom, pt1 = await ptOf(page, p.x, p.y);
    assert.ok(z1 > z0 * 1.3, `${z0} → ${z1}`);
    assert.ok(near(pt1[0], pt0[0], 0.05) && near(pt1[1], pt0[1], 0.05), 'about the fingers');
    await page.mouse.move(rxR.l + 200, rxR.t + 5);
    await page.keyboard.down('Control'); await page.mouse.wheel(0, -8); await page.keyboard.up('Control'); await settle(page);
    assert.ok((await C(page)).zoom > z1, 'over the ruler');
    assert.equal(await page.evaluate(() => window.visualViewport ? window.visualViewport.scale : 1), 1, 'the page is not zoomed');
    await page.keyboard.press('Control+0'); await settle(page);
  });

  /* ════ 4 · pan ════ */
  await step('4.1 once zoomed in, Space-drag, the Hand (H), the middle button and a trackpad\'s two-finger scroll each move the view by the hand\'s travel', async () => {
    await page.keyboard.press('Escape'); await settle(page);
    await page.evaluate(() => window.CharmEditor.current.setZoom(1600)); await settle(page);
    const ref = async () => clientOf(page, 100 + 12 * PT, 50 + 15 * PT);
    const c = { x: stageR.l + stageR.w / 2, y: stageR.t + stageR.h / 2 };
    const out = [];
    const check = async (name, fn, dx, dy) => { const a = await ref(); await fn(); const b = await ref(); out.push(`${name} ${Math.round(b.x - a.x)},${Math.round(b.y - a.y)}`); assert.ok(near(b.x - a.x, dx, 0.6) && near(b.y - a.y, dy, 0.6), `${name}: moved ${b.x - a.x}, ${b.y - a.y}; want ${dx}, ${dy}`); };
    // (mid-pan the charm is slid as drawn, not drawn again: a day square's path is on screen exactly where the view says it is, mid-way and after)
    const onScreen = async () => page.evaluate(() => { const C = window.CharmEditor.current, b = C.box('day-2-2'), a = C.clientOf(b[0], b[1]), r = document.querySelector('.ce-items path[data-id="day-2-2"]').getBoundingClientRect(); return { dx: r.left - a.x, dy: r.top - a.y, sliding: document.querySelector('.ce-stage').classList.contains('is-fastpan') }; });
    const r0 = await onScreen(); let mid = null;
    await check('space', async () => {
      await page.keyboard.down(' '); await page.mouse.move(c.x, c.y); await page.mouse.down(); await page.mouse.move(c.x + 60, c.y - 35, { steps: 5 }); await settle(page);
      mid = await onScreen();
      await page.mouse.move(c.x + 120, c.y - 70, { steps: 5 }); await page.mouse.up(); await page.keyboard.up(' '); await settle(page);
    }, 120, -70);
    const r1 = await onScreen();
    assert.ok(mid.sliding && !r1.sliding, 'slid while panning, drawn afresh after');
    for (const r of [mid, r1]) assert.ok(near(r.dx, r0.dx, 0.6) && near(r.dy, r0.dy, 0.6), `drawn where the view says: ${JSON.stringify(r)} vs ${JSON.stringify(r0)}`);
    assert.deepEqual((await C(page)).sel, [], 'a pan selects nothing');
    await page.keyboard.press('h'); assert.equal((await C(page)).tool, 'hand');
    await check('hand', () => drag(page, c, { x: c.x - 90, y: c.y + 40 }), -90, 40);
    await page.keyboard.press('v');
    await check('middle', () => drag(page, c, { x: c.x + 33, y: c.y + 55 }, { button: 'middle' }), 33, 55);
    await page.mouse.move(c.x, c.y);
    // (a trackpad's scroll comes in pixels and slanted; a synthetic one straight down reads as a mouse wheel's notch, which zooms, so this one is slanted)
    await check('trackpad', async () => { await page.mouse.wheel(30, 45); await page.mouse.wheel(-12, 20); await settle(page); await page.waitForTimeout(200); await settle(page); }, -18, -65);
    assert.ok(!(await page.evaluate(() => document.querySelector('.ce-stage').classList.contains('is-fastpan'))), 'drawn afresh once the scroll stops');
    assert.equal((await C(page)).zoom, 1600, 'the zoom is untouched');
    return out.join(' · ');
  });
  assert.deepEqual(page.__errs, []);
  await page.context().close();

  /* ════ 5 · frame work on a 300-path charm ════ */
  {
    const page = await newPage(1280, 720);
    await page.evaluate(() => {
      // 300 parts: 100 letter outlines of the stand-in font, 100 circles of four curves, 100 hearts of six; a 40 × 50 mm charm
      const PT = 72 / 25.4, K = 0.5522847498, P = (x, y) => [+(x * PT).toFixed(4), +(y * PT).toFixed(4)], its = [];
      its.push({ id: 'cut', kind: 'path', role: 'cut', source: 'master', stroke: true, fill: false, subpaths: [[['m', P(0.5, 0.5)], ['l', P(39.5, 0.5)], ['l', P(39.5, 49.5)], ['l', P(0.5, 49.5)], ['h']]] });
      const ABC = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
      for (let i = 0; i < 299; i++) {
        const col = i % 13, row = Math.floor(i / 13), x = 2 + col * 2.9, y = 2 + row * 2.05, kind = i % 3;
        let sp;
        if (kind === 0) { const g = CharmEditor.builtinTextToPaths(ABC[i % ABC.length], 'sans', 5, { align: 'left' }); sp = g.subpaths.map(s => s.map(op => op.map(v => (Array.isArray(v) ? [v[0] + x * PT, v[1] + (y + 1.6) * PT] : v)))); }
        else if (kind === 1) { const r = 0.8, cx = x + 1, cy = y + 0.9, k = r * K; sp = [[['m', P(cx + r, cy)], ['c', P(cx + r, cy + k), P(cx + k, cy + r), P(cx, cy + r)], ['c', P(cx - k, cy + r), P(cx - r, cy + k), P(cx - r, cy)], ['c', P(cx - r, cy - k), P(cx - k, cy - r), P(cx, cy - r)], ['c', P(cx + k, cy - r), P(cx + r, cy - k), P(cx + r, cy)], ['h']]]; }
        else { const q = (a, b) => P(x + 1 + a * 0.8, y + 0.9 + b * 0.8); sp = [[['m', q(0, 0.9)], ['c', q(-0.6, 0.45), q(-1, 0.05), q(-1, -0.35)], ['c', q(-1, -0.8), q(-0.55, -1), q(-0.3, -1)], ['c', q(-0.12, -1), q(0, -0.85), q(0, -0.65)], ['c', q(0, -0.85), q(0.12, -1), q(0.3, -1)], ['c', q(0.55, -1), q(1, -0.8), q(1, -0.35)], ['c', q(1, 0.05), q(0.6, 0.45), q(0, 0.9)], ['h']]]; }
        its.push({ id: 'p' + i, kind: 'path', role: kind === 0 ? 'hatch' : kind === 1 ? 'line' : 'art', source: 'master', stroke: kind === 1, fill: kind !== 1, lwPt: 0.25, subpaths: sp });
      }
      window.__big = { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 40 * PT, hPt: 50 * PT }, items: its };
    });
    await page.evaluate(() => { window.openEditor({ faces: { front: window.__big }, lockRoles: [] }); });
    await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page);
    const stage = await rect(page, '.ce-stage');
    const nPaths = await page.evaluate(() => document.querySelectorAll('.ce-items path').length);
    const cdp = await page.context().newCDPSession(page); await cdp.send('Performance.enable', { timeDomain: 'threadTicks' });   // (the main thread's own CPU time: a loaded machine does not inflate it)
    const metrics = async () => Object.fromEntries((await cdp.send('Performance.getMetrics')).metrics.map(m => [m.name, m.value]));
    /** Each step: one input event and one frame; the main thread's task time of that step (ms). */
    const frames = async (n, act) => {
      const per = [];
      for (let i = 0; i < n; i++) { const a = await metrics(); await act(i); await page.evaluate(() => new Promise(r => requestAnimationFrame(() => setTimeout(r, 0)))); const b = await metrics(); per.push((b.TaskDuration - a.TaskDuration) * 1000); }
      per.sort((x, y) => x - y);
      return { med: per[Math.floor(n / 2)], p90: per[Math.floor(n * 0.9)], max: per[n - 1], over: per.filter(v => v > 16).length };
    };
    const fmt = r => `median ${r.med.toFixed(2)} ms, p90 ${r.p90.toFixed(2)}, max ${r.max.toFixed(2)}, over 16 ms: ${r.over}`;
    const c = { x: stage.l + stage.w / 2, y: stage.t + stage.h / 2 };
    const pan = async () => { await page.keyboard.down(' '); await page.mouse.move(c.x, c.y); await page.mouse.down(); const r = await frames(60, i => page.mouse.move(c.x + Math.sin(i / 6) * 160, c.y + Math.cos(i / 7) * 110)); await page.mouse.up(); await page.keyboard.up(' '); return r; };
    const zoomW = async () => { await page.mouse.move(c.x + 40, c.y - 30); return frames(40, i => page.mouse.wheel(0, i % 20 < 10 ? -100 : 100)); };
    await page.evaluate(() => window.CharmEditor.current.setZoom(800)); await settle(page);
    const pan0 = await pan(), zoom0 = await zoomW();
    await page.keyboard.press('Control+a'); await settle(page);
    const nSel = (await C(page)).sel.length;
    await page.evaluate(() => window.CharmEditor.current.setZoom(800)); await settle(page);
    const pan1 = await pan(), zoom1 = await zoomW();
    await step(`5.1 a ${nPaths}-path charm at 800 %: every frame of a pan is under 16 ms of main-thread work, nothing selected and ${nSel} selected`, async () => {
      assert.ok(nPaths >= 300, 'paths ' + nPaths); assert.ok(nSel >= 299, 'selected ' + nSel);
      assert.ok(pan0.p90 < 16 && pan1.p90 < 16, `pan: ${fmt(pan0)} / ${fmt(pan1)}`);
      assert.ok(pan0.over <= 3 && pan1.over <= 3, 'frames over budget');
      return `pan, none selected: ${fmt(pan0)} · all selected: ${fmt(pan1)}`;
    });
    await step('5.2 and every frame of a wheel zoom on it too', async () => {
      assert.ok(zoom0.p90 < 16 && zoom1.p90 < 16, `zoom: ${fmt(zoom0)} / ${fmt(zoom1)}`);
      return `zoom, none selected: ${fmt(zoom0)} · all selected: ${fmt(zoom1)}`;
    });
    assert.deepEqual(page.__errs, []);
    await page.context().close();
  }

  await browser.close();
  console.log(`\n${results.length} passed, ${failures.length} failed`);
  if (failures.length) { console.log(failures.map(f => '  - ' + f).join('\n')); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
