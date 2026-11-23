// ISLANDS (Paul, 22 Nov 2026: "in the charm editor, I can't seem to ungroup a vectorized image which has several individual pieces in it that are not connected like words or letters,
// and I should be able to ungroup those ... Make sure the group works like an illustrator with the same keyboard shortcut in the same placement ... same icon"). No server, no network.
//   N1 a path of separate pieces (the outlines of "OAB 8") is found as its pieces: O with its hole, A with its counter, B with its two, 8 with its two; one ring is one piece
//   N2 an outline standing inside a hole (a dot inside an O) is a piece of its own
//   N3 the split keeps every ring exactly, each piece in its own path with the path's paint, role and transform, in its place in the stack
//   B  in the editor (Chromium): such a path selected lights Ungroup (the Control bar's button and the Object menu, ⇧⌘G); ⇧⌘G makes one path per piece (one undo step), drawn exactly
//      where the path was; a group still ungroups as before; a path of one piece is left as it is
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/ce-ungroup-pieces.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const CE = require(path.join(root, 'charm-nest-charm-editor.js'));
const M = CE.model, geo = M.createGeo();
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n         ')); };
function check(name, fn) { try { fn(); pass(name); } catch (e) { fail(name, e); } }
async function step(name, fn) { try { await fn(); pass(name); } catch (e) { fail(name, e); } }
const ring = (cx, cy, r, cw) => { const k = 0.5523 * r, s = cw ? -1 : 1; return [['m', [cx + r, cy]], ['c', [cx + r, cy + s * k], [cx + k, cy + s * r], [cx, cy + s * r]], ['c', [cx - k, cy + s * r], [cx - r, cy + s * k], [cx - r, cy]], ['c', [cx - r, cy - s * k], [cx - k, cy - s * r], [cx, cy - s * r]], ['c', [cx + k, cy - s * r], [cx + r, cy - s * k], [cx + r, cy]], ['h']]; };
const box = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']];

console.log('ce-ungroup-pieces · N (node)');
const words = geo.engine('OAB 8', 'sans', 24, { align: 'left' }).subpaths;
check('N1 the outlines of "OAB 8" are found as their 4 letters (the stand-in font draws a letter of bars that cross: one piece each), every ring in exactly one', () => {
  const g = M.islandsOf(words);
  assert.equal(g.length, 4, JSON.stringify(g));
  const real = M.islandsOf([ring(30, 50, 20), ring(30, 50, 12, true), ring(90, 50, 20), ring(90, 40, 6, true), ring(90, 60, 6, true)]);
  assert.deepEqual(real, [[0, 1], [2, 3, 4]], 'an O with its hole, an 8 with its two: ' + JSON.stringify(real));
  assert.equal(g.flat().length, words.length, 'every ring in exactly one piece');
  assert.equal(M.islandsOf([box(0, 0, 10, 10)]).length, 1, 'one ring: one piece');
});
check('N2 an outline standing inside a hole (a dot inside an O) is a piece of its own', () => {
  const g = M.islandsOf([ring(50, 50, 20), ring(50, 50, 14, true), ring(50, 50, 4)]);
  assert.deepEqual(g, [[0, 1], [2]]);
});
check('N3 the split: every ring kept exactly, one path a piece with the path\'s paint, role and transform, in its place in the stack', () => {
  const items = [{ id: 'a', kind: 'path', role: 'cut', subpaths: [box(0, 0, 100, 100)], stroke: true, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 },
    { id: 'w', kind: 'path', role: 'hatch', name: 'Traced', fill: true, evenOdd: true, subpaths: words, tx: 5, ty: 7, sx: 1.2, sy: 1.2, rot: 10 },
    { id: 'z', kind: 'path', role: 'line', subpaths: [box(1, 1, 2, 2)], stroke: true, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }];
  const r = M.splitIslands(items, 'w', geo);
  assert.equal(r.ids.length, 4); assert.deepEqual(r.items.map(i => i.id).slice(0, 1).concat(r.items.slice(-1).map(i => i.id)), ['a', 'z'], 'in its place in the stack');
  const parts = r.items.filter(i => r.ids.includes(i.id));
  assert.ok(parts.every(p => p.role === 'hatch' && p.fill && p.evenOdd && p.tx === 0 && p.sx === 1 && p.rot === 0), 'the paint and role kept, the transform put into the points');
  const W = geo.matrix(items[1]), want = M.transformSubpaths(M.islandsOf(words).flat().map(i => words[i]), W);
  assert.equal(JSON.stringify(parts.flatMap(p => p.subpaths)), JSON.stringify(want), 'every ring exactly, where it was drawn');
  assert.equal(M.splitIslands(items, 'a', geo).ids.length, 0, 'a path of one piece is left as it is');
  assert.ok(M.ungroupable(items[1]) && !M.ungroupable(items[0]) && M.ungroupable({ kind: 'group', children: [] }), 'what Ungroup takes apart');
});

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return done(); }
  console.log('ce-ungroup-pieces · B (Chromium)');
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM, args: ['--no-sandbox'] });
  const ctx0 = await browser.newContext({ viewport: { width: 1280, height: 760 }, reducedMotion: 'reduce' });
  await ctx0.route(u => !u.href.startsWith('file://'), r => r.abort());
  const page = await ctx0.newPage(), errs = [];
  page.on('pageerror', e => errs.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error') errs.push('console: ' + m.text()); });
  await page.goto('file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html'));
  const settle = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  const H = (fn, arg) => page.evaluate(fn, arg);
  const cur = expr => page.evaluate(`(() => { const C = window.CharmEditor.current; return ${expr}; })()`);
  await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle(); await settle();
  const fr = await cur('C.frame()');
  const id = await H(([fr, sps]) => { const c = window.CharmEditor.current.ctx; const ids = c.addItems([{ kind: 'path', role: 'hatch', name: 'Traced picture', fill: true, evenOdd: true, subpaths: sps, tx: fr.x0 + 4, ty: fr.y0 + fr.hPt / 2, sx: 0.6, sy: 0.6, rot: 0 }]); c.commit(); c.select(ids); return ids[0]; }, [fr, words]);
  await settle();
  const boxBefore = await cur(`C.box(${JSON.stringify(id)})`);
  await step('B1 a traced path of separate pieces lights Ungroup: the Control bar\'s button and the Object menu (⇧⌘G)', async () => {
    const lit = await H(() => { const b = document.querySelector('[data-ctl-ungroup]'); return !!b && !b.disabled; });
    assert.ok(lit, 'the Control bar\'s Ungroup');
    const t = await H(() => { const b = document.querySelector('[data-ctl-ungroup]'); return b && b.getAttribute('title'); });
    assert.ok(/Ungroup \(.*G\)/.test(String(t)), 'its tooltip names the key: ' + t);
  });
  await step('B2 ⇧⌘G makes one path per piece (4), one undo step, drawn exactly where the path was', async () => {
    const h0 = (await cur('C.history()')).undo;
    await page.keyboard.press(process.platform === 'darwin' ? 'Meta+Shift+G' : 'Control+Shift+G'); await settle();
    const sel = await cur('C.selection()'), items = await cur('C.items()');
    assert.equal(sel.length, 4, 'four pieces selected: ' + JSON.stringify(sel));
    assert.ok(!items.some(i => i.id === id), 'the path is gone');
    assert.equal((await cur('C.history()')).undo, h0 + 1, 'one undo step');
    const boxes = await H(ids => ids.map(i => window.CharmEditor.current.box(i)), sel), u = boxes.reduce((a, b) => [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);
    assert.ok(u.every((v, i) => Math.abs(v - boxBefore[i]) < 0.01), `where it was: ${JSON.stringify(u)} vs ${JSON.stringify(boxBefore)}`);
    await page.keyboard.press(process.platform === 'darwin' ? 'Meta+z' : 'Control+z'); await settle();
    assert.ok((await cur('C.items()')).some(i => i.id === id), 'undone: the one path is back');
  });
  await step('B3 a group still ungroups as before; a path of one piece leaves Ungroup off', async () => {
    const ids = await H(fr => { const c = window.CharmEditor.current.ctx; return c.addItems([{ kind: 'path', role: 'hatch', fill: true, subpaths: [[['m', [0, 0]], ['l', [10, 0]], ['l', [10, 10]], ['h']]], tx: fr.x0 + 2, ty: fr.y0 + 2, sx: 1, sy: 1, rot: 0 }, { kind: 'path', role: 'hatch', fill: true, subpaths: [[['m', [20, 0]], ['l', [30, 0]], ['l', [30, 10]], ['h']]], tx: fr.x0 + 2, ty: fr.y0 + 2, sx: 1, sy: 1, rot: 0 }]); }, fr);
    await H(ids => window.CharmEditor.current.ctx.select(ids), ids); await settle();
    await page.keyboard.press(process.platform === 'darwin' ? 'Meta+g' : 'Control+g'); await settle();
    const g = (await cur('C.selection()'))[0], gi = (await cur('C.items()')).find(i => i.id === g);
    assert.equal(gi && gi.kind, 'group');
    await page.keyboard.press(process.platform === 'darwin' ? 'Meta+Shift+G' : 'Control+Shift+G'); await settle();
    assert.equal((await cur('C.selection()')).length, 2, 'the group ungrouped');
    await H(id => window.CharmEditor.current.ctx.select([id]), (await cur('C.selection()'))[0]); await settle();
    assert.ok(await H(() => { const b = document.querySelector('[data-ctl-ungroup]'); return !!b && b.disabled; }), 'one piece: Ungroup off');
  });
  await step('B4 no page error', async () => { assert.deepEqual(errs, []); });
  await browser.close(); done();
})().catch(e => { fail('B', e); done(); });
function done() { console.log(failures.length ? `FAIL ce-ungroup-pieces (${failures.length}): ${failures.join(' | ')}` : `PASS ce-ungroup-pieces (${results.length})`); process.exit(failures.length ? 1 : 0); }
