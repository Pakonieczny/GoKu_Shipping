// DEFORMING LINES IN THE CHARM EDITOR (charm-nest-ce-paths.js; Paul, 28 Oct 2026: "designing custom charms, which requires blinds and deforming lines basically a (Designer) of
// a charm would use to manipulate a vector illustrator file"; the lead's D3: anchors, curve handles, the pen). The Pen, Direct Selection, the anchor tools, Scissors, Join, Close,
// Simplify, Smooth and the Pencil, done the way a person does them: the mouse, the keys, the menus. No server, no network, no live service.
//   $SP/int/slot.sh node tests/charm-nest/ce-paths.cjs          (CEP_ONLY=A|B|C; SHOTS=<dir> keeps screenshots)
// A (node) the model: contours read and written back exactly; an anchor added where a curve is split leaves the shape as it was, one taken out refits the two curves as one;
//   Simplify keeps corners and the drawing within its tolerance; line soup (an imported DXF's hundreds of short lines) joins into whole paths and closes the loops that nearly
//   close; the Pencil keeps sharp turns; a part's transform is kept when its geometry changes (the anchors nobody touched stay put on the charm)
// B (the editor, fixtures/charm-editor-harness.html) the tools on the rail and their keys; a closed pen shape with corners, smooth curves, a broken handle and a 45° anchor, one
//   undo step per click, every move's work measured; Direct Selection: an anchor dragged exactly with the pointer, a smooth handle turning its partner, Alt breaking it, a sweep
//   of anchors, arrows (a run of taps one undo step); the Anchor Point, Delete and Add anchor tools; a master's part turned 30° edited where it is drawn (its turn kept, its role
//   kept); the locked cut line out of reach; Scissors and Join on a master's square; Join on line soup; Simplify (preview, Apply one step, Cancel nothing); the Pencil and Smooth
// C the calendar master (_artCalendar.cjs) opened in the editor as the Engraving window opens it (CharmNestArtEdit.itemsOf), a day square's corner dragged and a curved pen shape
//   drawn, Saved: the bridge's charmEditChanges (sliced from charm-nest-bridge.js) -> CharmNestArtEdit.encode -> apply puts that very geometry on the piece
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const ROOT = path.join(__dirname, '../..');
const ONLY = process.env.CEP_ONLY || '';
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 10).join('\n         ')); };
async function step(name, fn) { try { const said = await fn(); pass(name + (said ? '  [' + said + ']' : '')); } catch (e) { fail(name, e); } }
const near = (a, b, eps) => Math.abs(a - b) <= eps;
const PT = 72 / 25.4;
const P = require(path.join(ROOT, 'charm-nest-ce-paths.js')), PM = P.model, CE = require(path.join(ROOT, 'charm-nest-charm-editor.js')), CM = CE.model;

/* the largest distance from any point of contours A to the drawing of contours B (A sampled, B as fine chords) */
function deviation(A, B, step) {
  const a = [].concat(...A.map(c => PM.sampleContour(c, step))), segs = [];
  for (const c of B) { const q = PM.sampleContour(c, step / 4); for (let i = 1; i < q.length; i++) segs.push([q[i - 1], q[i]]); if (c.closed && q.length > 1) segs.push([q[q.length - 1], q[0]]); if (q.length === 1) segs.push([q[0], q[0]]); }
  let worst = 0;
  for (const p of a) {
    let best = Infinity;
    for (const [u, v] of segs) { const dx = v[0] - u[0], dy = v[1] - u[1], L = dx * dx + dy * dy; let t = L ? ((p[0] - u[0]) * dx + (p[1] - u[1]) * dy) / L : 0; t = Math.max(0, Math.min(1, t)); const d = Math.hypot(p[0] - u[0] - t * dx, p[1] - u[1] - t * dy); if (d < best) best = d; }
    if (best > worst) worst = best;
  }
  return worst;
}
const K = 0.5522847498;
const circleSP = (cx, cy, r) => { const k = r * K; return [[['m', [cx + r, cy]], ['c', [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ['c', [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ['c', [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ['c', [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]], ['h']]]; };
const ngonSP = (cx, cy, r, n, wob) => { const sp = []; for (let i = 0; i < n; i++) { const a = i / n * 2 * Math.PI, rr = r + (wob || 0) * Math.sin(a * 23); sp.push([i ? 'l' : 'm', [cx + rr * Math.cos(a), cy + rr * Math.sin(a)]]); } sp.push(['h']); return [sp]; };

/* ═══ A · the model ═══ */
async function partA() {
  console.log('A · the model');
  const c0 = circleSP(50, 50, 20), cs = PM.toContours(c0);
  await step('A1 a closed curve reads as 4 anchors (its explicit last point folded into its first) and an unedited contour is written back as the very ops it came from', async () => {
    assert.equal(cs.length, 1); assert.equal(cs[0].pts.length, 4); assert.ok(cs[0].closed);
    assert.equal(PM.toSubpaths(cs)[0], c0[0]);
    const again = PM.toSubpaths([{ closed: true, pts: cs[0].pts }]);
    assert.equal(again[0].length, c0[0].length); assert.ok(deviation(PM.toContours(again), cs, 0.05) < 1e-9);
  });
  await step('A2 Add anchor splits a curve exactly (the shape unchanged to 0.0002 pt, the chords it is measured with); Delete anchor refits the two curves as one (within a hundredth of a point of where they were)', async () => {
    const s = PM.splitSeg(cs[0], 1, 0.3), d1 = deviation([s.c], cs, 0.05), back = PM.removeAnchor(s.c, s.k), d2 = deviation([back], cs, 0.05);
    assert.equal(s.c.pts.length, 5); assert.ok(d1 < 2e-4, 'split moved the shape ' + d1); assert.equal(back.pts.length, 4); assert.ok(d2 < 0.01, 'refit ' + d2);
    return `split ${d1.toExponential(1)} pt · refit ${d2.toFixed(4)} pt`;
  });
  await step('A3 Simplify: a 360-sided circle (as a DXF draws one) to a handful of curves within 0.05 pt; a square drawn in 160 tiny steps back to its 4 corners, straight', async () => {
    const pc = PM.toContours(ngonSP(50, 50, 20, 360))[0];
    let t = process.hrtime.bigint(); const sc = PM.simplifyContour(pc, 0.05); const ms = Number(process.hrtime.bigint() - t) / 1e6;
    const dv = Math.max(deviation([sc], [pc], 0.05), deviation([pc], [sc], 0.05));
    const sq = [[['m', [0, 0]]]]; for (let i = 1; i <= 40; i++) sq[0].push(['l', [i, 0]]); for (let i = 1; i <= 40; i++) sq[0].push(['l', [40, i]]); for (let i = 39; i >= 0; i--) sq[0].push(['l', [i, 40]]); for (let i = 39; i >= 1; i--) sq[0].push(['l', [0, i]]); sq[0].push(['h']);
    const s2 = PM.simplifyContour(PM.toContours(sq)[0], 0.05);
    assert.ok(sc.pts.length <= 12, 'anchors ' + sc.pts.length); assert.ok(dv <= 0.06, 'deviation ' + dv);
    assert.deepEqual(s2.pts.map(a => a.p.map(v => Math.round(v * 1e6) / 1e6)), [[0, 0], [40, 0], [40, 40], [0, 40]]); assert.ok(s2.pts.every(a => !a.i && !a.o));
    return `360 -> ${sc.pts.length} anchors, ${dv.toFixed(3)} pt off, ${ms.toFixed(1)} ms (first run) · square 160 -> 4`;
  });
  await step('A4 line soup: a circle as 360 separate lines (shuffled, a third reversed, ends 0.004 apart) and a 20-piece straight line join into ONE closed and ONE open path; a gap of 2 is never bridged', async () => {
    const segs = []; for (let i = 0; i < 360; i++) { const a = i * Math.PI / 180, b = (i + 1) * Math.PI / 180; let s = [['m', [50 + 20 * Math.cos(a) + 0.004, 50 + 20 * Math.sin(a)]], ['l', [50 + 20 * Math.cos(b), 50 + 20 * Math.sin(b)]]]; if (i % 3 === 0) s = [['m', s[1][1]], ['l', s[0][1]]]; segs.push(s); }
    for (let i = segs.length - 1; i > 0; i--) { const j = (i * 7919) % (i + 1); [segs[i], segs[j]] = [segs[j], segs[i]]; }
    const line = []; for (let i = 0; i < 20; i++) line.push([['m', [100 + i, 0]], ['l', [101 + i, 0]]]);
    const gap = [[['m', [0, 100]], ['l', [10, 100]]], [['m', [12, 100]], ['l', [20, 100]]]];
    const t = process.hrtime.bigint(), r = PM.chainSubpaths(segs.concat(line, gap), 0.01), ms = Number(process.hrtime.bigint() - t) / 1e6;
    const out = PM.toContours(r.subpaths), loops = out.filter(c => c.closed), opens = out.filter(c => !c.closed);
    assert.equal(loops.length, 1); assert.equal(loops[0].pts.length, 360); assert.equal(opens.length, 3); assert.ok(opens.some(c => c.pts.length === 21));
    assert.equal(r.joins, 359 + 19); assert.equal(r.closes, 1);
    return `382 pieces -> 4 paths (1 closed), ${r.joins} joins, ${ms.toFixed(1)} ms`;
  });
  await step('A5 Pencil fit: an L drawn freehand keeps its sharp corner (3 anchors, straight); a wobbly loop closes as a smooth closed path', async () => {
    const L = []; for (let i = 0; i <= 50; i++) L.push([i, 0]); for (let i = 1; i <= 50; i++) L.push([50, i]);
    const fl = PM.fitPoints(L, 0.2, false, 70);
    const ink = []; for (let i = 0; i <= 200; i++) { const a = i / 200 * 2 * Math.PI; ink.push([50 + 20 * Math.cos(a) + 0.15 * Math.sin(i * 1.7), 50 + 20 * Math.sin(a) + 0.15 * Math.cos(i * 2.3)]); }
    const fc = PM.fitPoints(ink, 0.3, true, 70), dv = deviation([fc], cs, 0.1);
    assert.deepEqual(fl.pts.map(a => a.p.map(v => Math.round(v * 100) / 100)), [[0, 0], [50, 0], [50, 50]]);
    assert.ok(fc.closed && fc.pts.length < 40 && dv < 0.5, JSON.stringify({ n: fc.pts.length, dv }));
    return `L: 3 anchors · loop: ${fc.pts.length} anchors, ${dv.toFixed(2)} pt from the true circle`;
  });
  await step('A6 a part turned 30° and scaled keeps its matrix when one anchor moves: every other anchor stays exactly where it was on the charm', async () => {
    const geo = CM.createGeo(null), it = { id: 'a', kind: 'path', subpaths: c0, tx: 3, ty: -2, sx: 1.5, sy: 0.7, rot: 30 };
    const W0 = geo.matrix(it), moved = PM.moveAnchors(cs[0], new Set([0]), [5, 0]), n = PM.rebuild(it, [moved], geo), W1 = geo.matrix(n);
    const c1 = PM.toContours(n.subpaths)[0];
    for (let k = 1; k < 4; k++) { const a = CM.ap(W0, ...cs[0].pts[k].p), b = CM.ap(W1, ...c1.pts[k].p); assert.ok(near(a[0], b[0], 1e-7) && near(a[1], b[1], 1e-7), 'anchor ' + k); }
    assert.ok(W0.every((v, i) => near(v, W1[i], 1e-7))); assert.equal(n.rot, 30); assert.equal(n.sx, 1.5);
  });
}

/* ═══ the browser ═══ */
let chromium = null;
try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(ROOT, 'node_modules'), 'playwright-core'))); } catch (_) { /* no browser here */ }
const HARNESS = 'file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html');
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
async function newPage(browser, w, h) {
  const ctx = await browser.newContext({ viewport: { width: w, height: h }, reducedMotion: 'reduce' });
  await ctx.route(u => !u.href.startsWith('file://'), r => r.abort());   // nothing leaves the machine
  const page = await ctx.newPage();
  page.__errs = [];
  page.on('pageerror', e => page.__errs.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error') page.__errs.push('console: ' + m.text()); });
  await page.goto(HARNESS);
  await page.addScriptTag({ path: path.join(ROOT, 'charm-nest-ce-paths.js') });
  await page.evaluate(installProbe);
  return page;
}
/* in the page: where the pointer went (in face pt, read from the editor itself), the main-thread work of every pointermove (from the window's first listener to the document's
   last: the editor's handler and everything it draws), and read-outs of a part's anchors in face pt */
function installProbe() {
  const ed = () => window.CharmEditor.current;
  window.__work = [];
  window.addEventListener('pointerdown', e => { if (ed()) window.__down = ed().ptOf(e.clientX, e.clientY); }, true);
  window.addEventListener('pointermove', e => { window.__t0 = performance.now(); if (ed()) window.__last = ed().ptOf(e.clientX, e.clientY); }, true);
  document.addEventListener('pointermove', () => { if (window.__t0 && window.__measure) window.__work.push(performance.now() - window.__t0); window.__t0 = 0; });
  window.addEventListener('pointerup', e => { if (ed()) window.__up = ed().ptOf(e.clientX, e.clientY); }, true);
  window.__pp = {
    W(id) { const ctx = ed().ctx, e = ctx.entry(id); return e ? ctx.model.mul(e.P, ctx.geo.matrix(e.it)) : null; },
    /** a part's contours in face pt (anchors and handles) */
    face(id) { const ctx = ed().ctx, e = ctx.entry(id); if (!e) return null; return CharmEditorPaths.model.contoursOf(e.it).map(c => CharmEditorPaths.model.mapC(c, this.W(id))); },
    it(id) { const e = ed().ctx.entry(id); return e ? e.it : null; },
    picked() { const st = CharmEditorPaths.state(); return st ? [...st.anc].filter(([, s]) => s.size).map(([id, s]) => [id, [...s].sort()]) : []; },
    added(before) { return ed().items().map(i => i.id).filter(id => !before.includes(id)); },
    dev(A, B, step) {
      const M = CharmEditorPaths.model, a = [].concat(...A.map(c => M.sampleContour(c, step))), segs = [];
      for (const c of B) { const q = M.sampleContour(c, step / 4); for (let i = 1; i < q.length; i++) segs.push([q[i - 1], q[i]]); if (c.closed && q.length > 1) segs.push([q[q.length - 1], q[0]]); }
      let worst = 0;
      for (const p of a) { let best = Infinity; for (const [u, v] of segs) { const dx = v[0] - u[0], dy = v[1] - u[1], L = dx * dx + dy * dy; let t = L ? ((p[0] - u[0]) * dx + (p[1] - u[1]) * dy) / L : 0; t = Math.max(0, Math.min(1, t)); best = Math.min(best, Math.hypot(p[0] - u[0] - t * dx, p[1] - u[1] - t * dy)); } worst = Math.max(worst, best); }
      return worst;
    }
  };
}
const F = page => page.evaluate(() => ({ X0: window.fixture.X0, Y0: window.fixture.Y0 }));
const mmPt = (x, y) => [100 + x * PT, 50 + y * PT];          // (the harness's frame: x0 100 pt, y0 50 pt)
const ptMm = p => [(p[0] - 100) / PT, (p[1] - 50) / PT];
const clientPt = (page, p) => page.evaluate(([x, y]) => window.CharmEditor.current.clientOf(x, y), p);
const clientMm = (page, x, y) => clientPt(page, mmPt(x, y));
const face = (page, id) => page.evaluate(id => window.__pp.face(id), id);
const hist = page => page.evaluate(() => window.CharmEditor.current.history().undo);
const sel = page => page.evaluate(() => window.CharmEditor.current.selection());
const tool = page => page.evaluate(() => window.CharmEditor.current.tool());
async function key(page, k) { await page.keyboard.press(k); await settle(page); }
async function clickC(page, c, mods) { for (const m of mods || []) await page.keyboard.down(m); await page.mouse.click(c.x, c.y); for (const m of (mods || []).slice().reverse()) await page.keyboard.up(m); await settle(page); }
async function dragC(page, a, b, mods, steps) {
  await page.mouse.move(a.x, a.y);
  for (const m of mods || []) await page.keyboard.down(m);
  await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: steps || 8 }); await page.mouse.up();
  for (const m of (mods || []).slice().reverse()) await page.keyboard.up(m);
  await settle(page);
}
const ptsOf = cs => [].concat(...cs.map(c => c.pts));
const same2 = (a, b, eps) => !!a && !!b && near(a[0], b[0], eps) && near(a[1], b[1], eps);
async function shot(page, name) { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, name) }); }

async function partB(browser) {
  console.log('B · the editor');
  const page = await newPage(browser, 1280, 720);
  await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page);
  const ids0 = (await page.evaluate(() => window.CharmEditor.current.items().map(i => i.id)));

  await step('B1 the rail: a Pen flyout (Pen, Add, Delete, Anchor Point), Direct Selection, a Pencil flyout (Pencil, Scissors); P A N C + − ⇧C = arm them; Esc goes back to Select', async () => {
    const rail = await page.evaluate(() => [...document.querySelectorAll('.ce-tools > .ce-tool, .ce-tools > .ce-drop > .ce-tool')].map(b => b.dataset.tool));
    const fly = await page.evaluate(() => [...document.querySelectorAll('.ce-rail .ce-menu [data-tool]')].map(b => b.dataset.tool));
    for (const id of ['pen', 'direct', 'pencil']) assert.ok(rail.includes(id), 'rail: ' + rail.join(','));
    for (const id of ['pen', 'anchorAdd', 'anchorDel', 'anchorConvert', 'pencil', 'scissors']) assert.ok(fly.includes(id), 'flyouts: ' + fly.join(','));
    const got = [];
    for (const [k, want] of [['p', 'pen'], ['a', 'direct'], ['n', 'pencil'], ['c', 'scissors'], ['+', 'anchorAdd'], ['-', 'anchorDel'], ['Shift+C', 'anchorConvert'], ['=', 'anchorAdd']]) { await key(page, k); got.push(k + '>' + await tool(page)); assert.equal(await tool(page), want, k); }
    await key(page, 'Escape'); assert.equal(await tool(page), 'select');
    return got.join(' ');
  });

  // ── the pen ──
  let penId = null;
  const P1 = [3, 11], P2 = [11, 10], P2o = [13, 10], P3 = [12, 17], P3o = [10, 19], P4 = [6, 18], P4o = [4, 16];
  await step('B2 the Pen draws a closed shape: a corner (click), two smooth anchors (drag: handles mirrored), a broken one (Alt-drag: the outgoing handle alone), a 45° anchor (Shift-click), closed on the ring; a red line engraving; ONE undo step per click', async () => {
    await key(page, 'p');
    const h0 = await hist(page);
    await page.evaluate(() => { window.__work = []; window.__measure = true; });
    await clickC(page, await clientMm(page, ...P1));
    await dragC(page, await clientMm(page, ...P2), await clientMm(page, ...P2o));
    const p2 = await page.evaluate(() => [window.__down, window.__up]);
    await dragC(page, await clientMm(page, ...P3), await clientMm(page, ...P3o));
    const p3 = await page.evaluate(() => [window.__down, window.__up]);
    await dragC(page, await clientMm(page, ...P4), await clientMm(page, ...P4o), ['Alt']);
    const p4 = await page.evaluate(() => [window.__down, window.__up]);
    await clickC(page, await clientMm(page, 2.4, 14.5), ['Shift']);
    // the ring on the first anchor when the pointer is over it, then the closing click
    const c1 = await clientMm(page, ...P1); await page.mouse.move(c1.x, c1.y); await settle(page);
    const ring = await page.evaluate(() => !!document.querySelector('.ce-ui .cep-ring'));
    await clickC(page, c1);
    const work = await page.evaluate(() => { window.__measure = false; return window.__work.slice(); });
    const added = await page.evaluate(b => window.__pp.added(b), ids0); penId = added[0];
    const it = await page.evaluate(id => window.__pp.it(id), penId), cs = await face(page, penId), h1 = await hist(page);
    assert.equal(added.length, 1); assert.equal(it.role, 'line'); assert.equal(it.stroke, true); assert.ok(ring, 'the closing ring');
    assert.equal(cs.length, 1); assert.ok(cs[0].closed, 'closed'); assert.equal(cs[0].pts.length, 5);
    const [a1, a2, a3, a4, a5] = cs[0].pts;
    assert.ok(!a1.i && !a1.o, 'P1 a corner');
    assert.ok(same2(a2.p, p2[0], 1e-6) && same2(a2.o, p2[1], 1e-6) && same2(a2.i, [2 * p2[0][0] - p2[1][0], 2 * p2[0][1] - p2[1][1]], 1e-6), 'P2 smooth: ' + JSON.stringify(a2));
    assert.ok(same2(a3.o, p3[1], 1e-6) && same2(a3.i, [2 * p3[0][0] - p3[1][0], 2 * p3[0][1] - p3[1][1]], 1e-6), 'P3 smooth');
    assert.ok(!a4.i && same2(a4.o, p4[1], 1e-6), 'P4 broken: ' + JSON.stringify(a4));
    assert.ok(near(Math.abs(a5.p[0] - a4.p[0]), Math.abs(a5.p[1] - a4.p[1]), 1e-6) && a5.p[0] < a4.p[0], 'P5 at 45° from P4');
    assert.equal(h1 - h0, 6, 'one step per click: ' + (h1 - h0));
    work.sort((a, b) => a - b);
    const med = work[work.length >> 1], p95 = work[Math.floor(work.length * 0.95)];
    assert.ok(p95 < 16, 'pointermove work p95 ' + p95);
    await shot(page, 'B2-pen.png');
    return `${work.length} moves: median ${med.toFixed(2)} ms, p95 ${p95.toFixed(2)} ms, max ${work[work.length - 1].toFixed(2)} ms of work · ${h1 - h0} undo steps`;
  });

  await step('B3 Direct Selection: a click picks one anchor (the bar offers corner / smooth / remove / cut); dragged, it lands exactly under the pointer and nothing else moves; ONE undo step', async () => {
    await key(page, 'a');
    const before = await face(page, penId), at = before[0].pts[0].p;
    await clickC(page, await clientPt(page, at));
    const picked = await page.evaluate(() => window.__pp.picked()), bar = await page.evaluate(() => [...document.querySelectorAll('.ce-xbar [data-xcmd]')].map(b => b.dataset.xcmd));
    assert.deepEqual(picked, [[penId, ['0:0']]]); assert.deepEqual(bar, ['anchorsCorner', 'anchorsSmooth', 'anchorsRemove', 'anchorsCut']);
    const h0 = await hist(page);
    await page.evaluate(() => { window.__work = []; window.__measure = true; });
    await dragC(page, await clientPt(page, at), await clientMm(page, 4, 12.5));
    const work = await page.evaluate(() => { window.__measure = false; return window.__work.slice().sort((a, b) => a - b); });
    const d = await page.evaluate(() => [window.__up[0] - window.__down[0], window.__up[1] - window.__down[1]]), after = await face(page, penId);
    assert.ok(same2(after[0].pts[0].p, [at[0] + d[0], at[1] + d[1]], 1e-6), 'moved with the pointer');
    for (let k = 1; k < 5; k++) assert.ok(same2(after[0].pts[k].p, before[0].pts[k].p, 1e-9), 'anchor ' + k + ' stayed');
    assert.equal(await hist(page) - h0, 1);
    return `max ${work[work.length - 1].toFixed(2)} ms of work a move`;
  });

  await step('B4 a smooth anchor\'s handle dragged turns its partner with it (still one line, its own length kept); Alt-dragged, the handle moves alone (the anchor becomes a corner)', async () => {
    let cs = await face(page, penId); const a2 = cs[0].pts[1];
    await clickC(page, await clientPt(page, a2.p));
    const iLen = Math.hypot(a2.i[0] - a2.p[0], a2.i[1] - a2.p[1]);
    await dragC(page, await clientPt(page, a2.o), await clientMm(page, 13, 8));
    cs = await face(page, penId); let b = cs[0].pts[1];
    const u = [b.o[0] - b.p[0], b.o[1] - b.p[1]], v = [b.p[0] - b.i[0], b.p[1] - b.i[1]], cross = (u[0] * v[1] - u[1] * v[0]) / (Math.hypot(...u) * Math.hypot(...v));
    assert.ok(Math.abs(cross) < 1e-6 && u[0] * v[0] + u[1] * v[1] > 0, 'still smooth: ' + cross);
    assert.ok(near(Math.hypot(...v), iLen, 1e-6), 'partner length kept');
    const o0 = b.o.slice();
    await dragC(page, await clientPt(page, b.i), await clientMm(page, 9, 11), ['Alt']);
    cs = await face(page, penId); b = cs[0].pts[1];
    assert.ok(same2(b.o, o0, 1e-9), 'the other handle stayed'); assert.ok(!(await page.evaluate(p => CharmEditorPaths.model.isSmooth(p), b)), 'now a corner');
    return `cross ${cross.toExponential(1)}`;
  });

  await step('B5 arrows nudge the picked anchor 0.05 mm a tap (Shift 1 mm); a run of taps is ONE undo step', async () => {
    let cs = await face(page, penId); const a3 = cs[0].pts[2];
    await clickC(page, await clientPt(page, a3.p));
    const h0 = await hist(page);
    for (let i = 0; i < 4; i++) await page.keyboard.press('ArrowRight');
    await page.keyboard.press('Shift+ArrowDown'); await settle(page);
    cs = await face(page, penId);
    const dmm = [(cs[0].pts[2].p[0] - a3.p[0]) / PT, (cs[0].pts[2].p[1] - a3.p[1]) / PT];
    assert.ok(near(dmm[0], 0.2, 1e-6) && near(dmm[1], 1, 1e-6), JSON.stringify(dmm));
    await page.waitForTimeout(1400); await settle(page);
    assert.equal(await hist(page) - h0, 1, 'one step for the run');
    return `moved ${dmm.map(v => v.toFixed(3)).join(', ')} mm`;
  });

  await step('B6 a sweep from empty space picks every anchor in the box (the pen shape\'s and the day squares\'), never the locked cut line\'s; Shift-click adds one', async () => {
    const a = await clientMm(page, 0.4, 16.6), b = await clientMm(page, 7.5, 20.2);
    await dragC(page, a, b);
    const picked = await page.evaluate(() => window.__pp.picked());
    const want = await page.evaluate(([x0, y0, x1, y1]) => {
      const out = []; const ctx = window.CharmEditor.current.ctx;
      const rec = list => list.forEach(it => { if (it.kind === 'group') return rec(it.children); if (it.kind !== 'path' || it.locked || it.hidden) return; const W = window.__pp.W(it.id); CharmEditorPaths.model.contoursOf(it).forEach((c, ci) => c.pts.forEach((p, ai) => { const q = ctx.model.ap(W, p.p[0], p.p[1]); if (q[0] >= x0 && q[0] <= x1 && q[1] >= y0 && q[1] <= y1) out.push(it.id + ':' + ci + ':' + ai); })); });
      rec(ctx.items()); return out.sort();
    }, [...mmPt(0.4, 16.6), ...mmPt(7.5, 20.2)]);
    const got = [].concat(...picked.map(([id, ks]) => ks.map(k => id + ':' + k))).sort();
    assert.deepEqual(got, want); assert.ok(got.some(k => k.startsWith(penId)) && got.some(k => k.startsWith('day-')) && !got.some(k => k.startsWith('cut-')));
    const cs = await face(page, penId);
    await clickC(page, await clientPt(page, cs[0].pts[1].p), ['Shift']);
    const p2 = await page.evaluate(() => window.__pp.picked());
    assert.ok(p2.find(([id]) => id === penId)[1].includes('0:1'), 'Shift-click added');
    return `${got.length} anchors in the box`;
  });

  await step('B7 the Anchor Point tool (⇧C): a click turns a smooth anchor into a corner; a drag from a corner pulls out smooth handles, mirrored', async () => {
    await key(page, 'Escape'); await clickC(page, await clientPt(page, (await face(page, penId))[0].pts[3].p));   // (Select picks the pen shape again)
    await key(page, 'Shift+C');
    let cs = await face(page, penId);
    await clickC(page, await clientPt(page, cs[0].pts[2].p));   // P3: smooth
    cs = await face(page, penId); assert.ok(!cs[0].pts[2].i && !cs[0].pts[2].o, 'P3 a corner now');
    await dragC(page, await clientPt(page, cs[0].pts[0].p), await clientMm(page, 5, 10));   // P1: a corner -> smooth
    const d = await page.evaluate(() => [window.__down, window.__up]);
    cs = await face(page, penId); const a = cs[0].pts[0];
    assert.ok(same2(a.o, d[1], 1e-6) && same2(a.i, [2 * a.p[0] - d[1][0], 2 * a.p[1] - d[1][1]], 1e-6), JSON.stringify(a));
  });

  await step('B8 the Delete Anchor tool (−) takes an anchor out and keeps the path closed; the Add Anchor tool (+) puts one on a segment, the shape unchanged', async () => {
    await key(page, '-');
    let cs = await face(page, penId);
    await clickC(page, await clientPt(page, cs[0].pts[4].p));
    cs = await face(page, penId); assert.equal(cs[0].pts.length, 4); assert.ok(cs[0].closed);
    await key(page, '+');
    const mid = await page.evaluate(id => { const c = window.__pp.face(id)[0], s = CharmEditorPaths.model.segOf(c, 1); return CharmEditorPaths.model.bez(...s.q, 0.5); }, penId);
    await clickC(page, await clientPt(page, mid));
    const after = await face(page, penId), dv = await page.evaluate(([a, b]) => window.__pp.dev(a, b, 0.02), [after, cs]);
    assert.equal(after[0].pts.length, 5); assert.ok(dv < 0.002, 'shape moved ' + dv);
    return `the shape within ${(dv / PT * 1000).toFixed(2)} µm`;
  });

  await step('B9 a master\'s part turned 30° (Heart outline, a red line): its anchor dragged where it is DRAWN lands under the pointer, the other anchors stay on the charm, its turn and its role stay', async () => {
    await key(page, 'Escape'); await key(page, 'Escape');
    const hb = await page.evaluate(() => window.CharmEditor.current.box('heart-line'));
    await page.evaluate(() => { const ctx = window.CharmEditor.current.ctx; ctx.select(['heart-line']); });
    const r = page.locator('.ce-root [data-f="r"]'); await r.click(); await page.keyboard.press('Control+a'); await page.keyboard.type('30'); await page.keyboard.press('Enter'); await settle(page);
    assert.equal((await page.evaluate(() => window.__pp.it('heart-line'))).rot, 30);
    await key(page, 'a');
    const before = await face(page, 'heart-line'), k = 2, at = before[0].pts[k].p;
    await clickC(page, await clientPt(page, at));
    await dragC(page, await clientPt(page, at), await clientPt(page, [at[0] + 1.2 * PT, at[1] + 0.6 * PT]));
    const d = await page.evaluate(() => [window.__up[0] - window.__down[0], window.__up[1] - window.__down[1]]), after = await face(page, 'heart-line'), it = await page.evaluate(() => window.__pp.it('heart-line'));
    assert.ok(same2(after[0].pts[k].p, [at[0] + d[0], at[1] + d[1]], 1e-6), 'under the pointer');
    let worst = 0; before[0].pts.forEach((a, i) => { if (i !== k) worst = Math.max(worst, Math.hypot(a.p[0] - after[0].pts[i].p[0], a.p[1] - after[0].pts[i].p[1])); });
    assert.ok(worst < 1e-6, 'others moved ' + worst); assert.equal(it.rot, 30); assert.equal(it.role, 'line'); assert.ok(hb);
    return `the others within ${worst.toExponential(1)} pt`;
  });

  await step('B10 the locked cut line is out of reach: a click on its anchor picks nothing of it and a drag from there only sweeps; it is not changed', async () => {
    const cut0 = await page.evaluate(() => JSON.stringify(window.__pp.it('cut-outline').subpaths));
    const cs = await face(page, 'cut-outline'), a = cs[0].pts[0].p;   // (its top edge's left end, clear of every other part)
    await clickC(page, await clientPt(page, a));
    assert.ok(!(await sel(page)).includes('cut-outline')); assert.ok(!(await page.evaluate(() => window.__pp.picked())).some(([id]) => id === 'cut-outline'));
    await dragC(page, await clientPt(page, a), await clientPt(page, [a[0] + 2 * PT, a[1] + 1 * PT]));
    assert.equal(await page.evaluate(() => JSON.stringify(window.__pp.it('cut-outline').subpaths)), cut0);
  });

  await step('B11 Scissors (C) cut a master\'s day square on its top edge: it opens there, its two ends on that very point, the drawing unchanged; Join (⌘J) on those ends closes it again', async () => {
    await key(page, 'Escape'); await key(page, 'c');
    const before = await face(page, 'day-0-0'), mid = [(before[0].pts[0].p[0] + before[0].pts[1].p[0]) / 2, before[0].pts[0].p[1]];
    await clickC(page, await clientPt(page, mid));
    const cut = await face(page, 'day-0-0'), n = cut[0].pts.length;
    assert.ok(!cut[0].closed); assert.equal(n, 6); assert.ok(same2(cut[0].pts[0].p, cut[0].pts[n - 1].p, 1e-9) && same2(cut[0].pts[0].p, mid, 0.05 * PT), 'ends on the cut');
    assert.ok((await page.evaluate(([a, b]) => window.__pp.dev(a, b, 0.05), [cut, before])) < 2e-4);
    const h0 = await hist(page);
    await key(page, 'Control+j');
    const joined = await face(page, 'day-0-0');
    assert.ok(joined[0].closed); assert.equal(joined[0].pts.length, 5); assert.equal(await hist(page) - h0, 1);
    assert.ok((await page.evaluate(([a, b]) => window.__pp.dev(a, b, 0.05), [joined, before])) < 2e-4);
  });

  await step('B12 Join on line soup: 36 short lines of a circle (as a DXF brings them, ends a hundredth of a mm apart), all selected, ⌘J: ONE closed path, the other parts gone, ONE undo step', async () => {
    await key(page, 'Escape'); await key(page, 'Escape');
    const ids = await page.evaluate(() => {
      const ctx = window.CharmEditor.current.ctx, PT = 72 / 25.4, cx = 100 + 12 * PT, cy = 50 + 20 * PT, r = 3 * PT, list = [];
      for (let i = 0; i < 36; i++) { const a = i / 36 * 2 * Math.PI, b = (i + 1) / 36 * 2 * Math.PI; list.push({ kind: 'path', role: 'line', name: 'DXF line', stroke: true, subpaths: [[['m', [cx + r * Math.cos(a) + 0.01 * PT, cy + r * Math.sin(a)]], ['l', [cx + r * Math.cos(b), cy + r * Math.sin(b)]]]] }); }
      const ids = ctx.addItems(list); ctx.commit('soup'); return ids;
    });
    await page.evaluate(ids => window.CharmEditor.current.ctx.select(ids), ids);
    const h0 = await hist(page), t0 = Date.now();
    await key(page, 'Control+j');
    const ms = Date.now() - t0, left = await page.evaluate(ids => ids.filter(id => window.__pp.it(id)), ids);
    assert.equal(left.length, 1); const cs = await face(page, left[0]);
    assert.equal(cs.length, 1); assert.ok(cs[0].closed); assert.equal(cs[0].pts.length, 36); assert.equal(await hist(page) - h0, 1);
    page.__soup = left[0];
    return `36 parts -> 1 closed path (${ms} ms for the key and two frames)`;
  });

  await step('B13 Simplify… from the Object menu: the panel says the anchors before and after, the result is drawn over the charm as the slider moves (the paths untouched until Apply); Cancel changes nothing; Apply is ONE undo step', async () => {
    const id = page.__soup, h0 = await hist(page), same0 = () => page.evaluate(() => window.CharmEditor.current.items() === window.__i0);
    await page.evaluate(() => { window.__i0 = window.CharmEditor.current.items(); });
    const openMenu = async () => { await page.click('.ce-root [data-drop="xcmd"] .ce-drop__b'); await settle(page); await page.click('.ce-root [data-xcmd="pathSimplify"]'); await settle(page); };
    await openMenu();
    const setMm = async v => { await page.evaluate(v => { const r = document.querySelector('.cep-pop input'); r.value = String((Math.log10(v) + 3) / 3); r.dispatchEvent(new Event('input')); }, v); await settle(page); };
    await setMm(0.05);
    const said = await page.evaluate(() => document.querySelector('.cep-pop__n').textContent), prev = await page.evaluate(() => document.querySelectorAll('.cep-prev .cep-prev__p').length);
    assert.equal(await same0(), true, 'nothing changed while previewing');
    await page.click('.cep-pop [data-x="no"]'); await settle(page);
    assert.equal(await same0(), true); assert.equal(await hist(page), h0); assert.equal(await page.evaluate(() => !!document.querySelector('.cep-pop, .cep-prev')), false);
    await openMenu(); await setMm(0.05); await page.click('.cep-pop [data-x="yes"]'); await settle(page);
    const after = await face(page, id), m = /(\d+)\s*→\s*(\d+)/.exec(said);
    assert.ok(m && +m[1] === 36 && +m[2] === after[0].pts.length && after[0].pts.length < 36, said + ' / ' + after[0].pts.length); assert.equal(prev, 1);
    assert.equal(await hist(page) - h0, 1);
    return `${said} · within 0.05 mm`;
  });

  await step('B14 the Pencil (N): a hand\'s wobbly stroke becomes a smooth red line of a few Béziers; a stroke ending at its start closes; Smooth irons a stroke, ONE undo step a press', async () => {
    await key(page, 'Escape'); await key(page, 'n');
    const b0 = await page.evaluate(() => window.CharmEditor.current.items().map(i => i.id));
    const pts = []; for (let i = 0; i <= 40; i++) pts.push(await clientMm(page, 3 + i * 0.45, 24.5 + 1.2 * Math.sin(i / 6) + 0.12 * ((i * 7) % 3 - 1)));
    await page.mouse.move(pts[0].x, pts[0].y); await page.mouse.down(); for (const p of pts.slice(1)) await page.mouse.move(p.x, p.y); await page.mouse.up(); await settle(page);
    const id = (await page.evaluate(b => window.__pp.added(b), b0))[0], it = await page.evaluate(id => window.__pp.it(id), id), cs = await face(page, id);
    assert.equal(it.role, 'line'); assert.equal(cs.length, 1); assert.ok(!cs[0].closed); assert.ok(cs[0].pts.length >= 2 && cs[0].pts.length <= 12, 'anchors ' + cs[0].pts.length); assert.ok(ptsOf(cs).some(a => a.i || a.o), 'curves');
    const loop = []; for (let i = 0; i <= 48; i++) { const a = i / 48 * 2 * Math.PI; loop.push(await clientMm(page, 19 + 2 * Math.cos(a), 24 + 2 * Math.sin(a))); }
    const b1 = await page.evaluate(() => window.CharmEditor.current.items().map(i => i.id));
    await page.mouse.move(loop[0].x, loop[0].y); await page.mouse.down(); for (const p of loop.slice(1)) await page.mouse.move(p.x, p.y); await page.mouse.up(); await settle(page);
    const lid = (await page.evaluate(b => window.__pp.added(b), b1))[0], lcs = await face(page, lid);
    assert.ok(lcs[0].closed, 'the loop closed'); assert.ok(lcs[0].pts.length <= 12, 'loop anchors ' + lcs[0].pts.length);
    await page.evaluate(id => window.CharmEditor.current.ctx.select([id]), id);
    const h0 = await hist(page);
    await page.click('.ce-root [data-drop="xcmd"] .ce-drop__b'); await settle(page); await page.click('.ce-root [data-xcmd="pathSmooth"]'); await settle(page);
    const sm = await face(page, id);
    assert.equal(await hist(page) - h0, 1); assert.ok(!sm[0].closed && sm[0].pts.length <= cs[0].pts.length + 2);
    assert.ok(same2(sm[0].pts[0].p, cs[0].pts[0].p, 1e-6) && same2(sm[0].pts[sm[0].pts.length - 1].p, cs[0].pts[cs[0].pts.length - 1].p, 1e-6), 'the ends stay');
    await shot(page, 'B14-pencil.png');
    return `stroke: 41 points -> ${cs[0].pts.length} anchors · loop ${lcs[0].pts.length} anchors, closed · smoothed ${sm[0].pts.length}`;
  });

  await step('B15 a big path (3000 anchors) stays light: an anchor dragged with Direct Selection costs one frame\'s work or less a move', async () => {
    await key(page, 'Escape'); await key(page, 'Escape');
    const id = await page.evaluate(() => {
      const ctx = window.CharmEditor.current.ctx, PT = 72 / 25.4, cx = 100 + 12 * PT, cy = 50 + 15 * PT, sp = [];
      for (let i = 0; i < 3000; i++) { const a = i / 3000 * 2 * Math.PI, r = (8 + 0.4 * Math.sin(a * 40)) * PT; sp.push([i ? 'l' : 'm', [cx + r * Math.cos(a), cy + r * Math.sin(a)]]); }
      sp.push(['h']); const ids = ctx.addItems([{ kind: 'path', role: 'line', name: 'Big', stroke: true, subpaths: [sp] }]); ctx.commit('big'); return ids[0];
    });
    await key(page, 'a');
    const at = (await face(page, id))[0].pts[750].p;
    await clickC(page, await clientPt(page, at));
    await page.evaluate(() => { window.__work = []; window.__measure = true; });
    await dragC(page, await clientPt(page, at), await clientPt(page, [at[0] + 2 * PT, at[1] + 1 * PT]), [], 20);
    const work = await page.evaluate(() => { window.__measure = false; return window.__work.slice().sort((a, b) => a - b); });
    const after = await face(page, id), d = await page.evaluate(() => [window.__up[0] - window.__down[0], window.__up[1] - window.__down[1]]);
    assert.ok(same2(after[0].pts[750].p, [at[0] + d[0], at[1] + d[1]], 1e-6));
    const p95 = work[Math.floor(work.length * 0.95)];
    assert.ok(p95 < 16, 'p95 ' + p95);
    await page.evaluate(id => { const ctx = window.CharmEditor.current.ctx; ctx.removeItems([id]); ctx.commit('x'); }, id);
    return `${work.length} moves: median ${work[work.length >> 1].toFixed(2)} ms, p95 ${p95.toFixed(2)} ms`;
  });

  await step('B16 Save: the result says the pen shape and the pencil lines were added and the heart, the day square changed, each with its geometry; no page error', async () => {
    await key(page, 'Escape');
    await page.click('.ce-root [data-act="save"]'); await page.waitForFunction(() => window.__closed === true);
    const r = await page.evaluate(() => window.__result), ch = r.changes.front;
    assert.ok(ch.added.some(a => a.id === penId), 'the pen shape added'); assert.ok(ch.changed['heart-line'] && ch.changed['day-0-0'], 'the heart and the square changed');
    const pen = r.faces.front.items.find(i => i.id === penId);
    assert.ok(Array.isArray(pen.matrix) && pen.subpaths.length === 1 && pen.subpaths[0][pen.subpaths[0].length - 1][0] === 'h');
    assert.deepEqual(page.__errs, []);
    return `added ${ch.added.length}, changed ${Object.keys(ch.changed).length}, removed ${ch.removed.length}`;
  });
  await page.context().close();
}

/* ═══ C · saved through the bridge's charmEditChanges and the art edit ═══ */
async function nodeKit() {
  const hadWindow = 'window' in global, hadSelf = 'self' in global;
  global.window = global; global.self = global;
  global.PDFLib = global.PDFLib || require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
  require(path.join(ROOT, 'charm-nest-pdf.js'));
  global.CharmNestGeom = global.CharmNestGeom || require(path.join(ROOT, 'charm-nest-geom.js'));
  const cal = await require('./_artCalendar.cjs')(ROOT);
  const kit = { G: global.CharmNestGeom, AE: require(path.join(ROOT, 'charm-nest-art-edit.js')), cal };
  if (!hadWindow) delete global.window; if (!hadSelf) delete global.self;   // (playwright must not think it runs in a page)
  return kit;
}
/** charmEditChanges as the bridge has it (sliced from charm-nest-bridge.js: the page's own code, not a copy), with the art edit and the editor's model as its window. */
function bridgeChanges(AE) {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const a = src.indexOf('  function charmEditChanges('), b = src.indexOf('\n  /** ', a + 10);
  if (a < 0 || b < 0) throw new Error('charmEditChanges not found in charm-nest-bridge.js');
  return new Function('window', src.slice(a, b) + '\n return charmEditChanges;')({ CharmNestArtEdit: AE, CharmEditor: { model: CM } });
}
async function partC(browser) {
  console.log('C · saved through charmEditChanges');
  const kit = await nodeKit(), { AE, G, cal } = kit, cb = cal.charm;
  const view = G.backView(cb, { face: 'front', upAngle: 75, res: 6 }), fr = AE.frame(view);
  const items = AE.itemsOf(cb, view, 'front').map(it => Object.assign({}, it, { kind: 'path', source: it.origin === 'added' ? 'added' : 'master' }));
  const outline = items.find(i => i.role === 'cut'), ob = CM.bboxOf(outline.subpaths);
  const docFront = { face: 'front', unitPt: true, frame: { x0: ob[0], y0: ob[1], wPt: ob[2] - ob[0], hPt: ob[3] - ob[1] }, items };
  const sqId = AE.memberIds(cb)[cal.day(1, 2)];
  const page = await newPage(browser, 1280, 720);
  await page.evaluate(doc => { window.openEditor({ faces: { front: doc }, startFace: 'front', title: 'CALENDAR' }); }, docFront);
  await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page);
  let penId = null, sqFace = null;
  await step('C1 in the editor: a day square of the master (turned 75° in its view) has a corner dragged 1 mm with Direct Selection, and a curved closed pen shape is drawn in another square', async () => {
    await page.evaluate(id => window.CharmEditor.current.ctx.select([id]), sqId);
    await key(page, 'a');
    const cs = await face(page, sqId), at = cs[0].pts[0].p;
    await clickC(page, await clientPt(page, at));
    await dragC(page, await clientPt(page, at), await clientPt(page, [at[0] + PT, at[1] + 0.5 * PT]));
    sqFace = await face(page, sqId);
    assert.ok(!same2(sqFace[0].pts[0].p, at, 0.5) && same2(sqFace[0].pts[1].p, cs[0].pts[1].p, 1e-9));
    await key(page, 'Escape'); await key(page, 'Escape'); await key(page, 'p');
    const b0 = await page.evaluate(() => window.CharmEditor.current.items().map(i => i.id));
    const box = await page.evaluate(id => window.CharmEditor.current.box(id), AE.memberIds(cb)[cal.day(2, 3)]);
    const cx = (box[0] + box[2]) / 2, cy = (box[1] + box[3]) / 2, r = (box[2] - box[0]) * 0.25;
    await clickC(page, await clientPt(page, [cx - r, cy]));
    await dragC(page, await clientPt(page, [cx, cy - r]), await clientPt(page, [cx + r * 0.8, cy - r]));
    await dragC(page, await clientPt(page, [cx + r, cy + r * 0.6]), await clientPt(page, [cx, cy + r * 1.2]));
    await clickC(page, await clientPt(page, [cx - r, cy]));
    penId = (await page.evaluate(b => window.__pp.added(b), b0))[0];
    const pc = await face(page, penId);
    assert.ok(pc[0].closed && pc[0].pts.length === 3 && ptsOf(pc).some(a => a.i || a.o));
  });
  await step('C2 Saved and passed through the bridge\'s charmEditChanges, then CharmNestArtEdit.encode and apply: the piece\'s square and the new shape are drawn exactly where the editor drew them (0.003 pt)', async () => {
    await key(page, 'Escape');
    await page.click('.ce-root [data-act="save"]'); await page.waitForFunction(() => window.__closed === true);
    const res = await page.evaluate(() => window.__result), d = res.changes.front;
    const charmEditChanges = bridgeChanges(AE), ch = charmEditChanges(cb, items, res.faces.front.items, d, 'front');
    assert.ok(ch.changed[sqId], 'the square is changed'); assert.ok(ch.added.some(a => a.id === penId), 'the shape is added'); assert.equal(ch.removed.length, 0);
    const rec = AE.encode(ch, { charm: cb, view, face: 'front', placed: false }), ed = AE.apply(cb, Object.assign({ at: 1 }, rec), 'front');
    const asItem = segs => segs.flatMap(m => m.subpaths.map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(fr.toItem))))));
    const drawn = id => { const it = res.faces.front.items.find(i => i.id === id); return CM.transformSubpaths(it.subpaths, it.matrix); };
    const sqIdx = AE.memberIds(cb).indexOf(sqId), idsAfter = AE.memberIds(ed);
    const gotSq = asItem(ed.members.filter((m, i) => m && (m.artMemberId === sqId || idsAfter[i] === sqId || (i === sqIdx && m.artChanged))));
    const gotPen = asItem(ed.members.filter(m => m && m.artMemberId === penId));
    const worst = (A, B) => { const a = PM.toContours(A), b = PM.toContours(B); if (!a.length || a.length !== b.length) return Infinity; return Math.max(deviation(a, b, 0.05), deviation(b, a, 0.05)); };
    const dSq = worst(gotSq, drawn(sqId)), dPen = worst(gotPen, drawn(penId));
    assert.ok(dSq < 0.003, 'square ' + dSq); assert.ok(dPen < 0.003, 'shape ' + dPen);
    // (and the square really moved: what the piece has is not the master's square)
    const master = asItem([cb.members[sqIdx]]); assert.ok(worst(gotSq, master) > 0.5 * PT);
    assert.ok(ed.members.find(m => m && m.artMemberId === penId).stroke, 'the shape is a stroked line');
    assert.deepEqual(page.__errs, []);
    return `square ${dSq.toExponential(1)} pt, shape ${dPen.toExponential(1)} pt off what the editor drew`;
  });
  await page.context().close();
}

(async () => {
  if (ONLY === '' || ONLY === 'A') await partA();
  if (ONLY !== 'A') {
    if (!chromium) console.log('  - no playwright-core: the browser checks were not run');
    else {
      const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
      try { if (ONLY === '' || ONLY === 'B') await partB(browser); if (ONLY === '' || ONLY === 'C') await partC(browser); } finally { await browser.close(); }
    }
  }
  console.log(`\n${results.length}/${results.length + failures.length} checks pass`);
  if (failures.length) { console.log(failures.map(f => '  - ' + f).join('\n')); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
