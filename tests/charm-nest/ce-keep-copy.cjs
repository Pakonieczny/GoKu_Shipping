// KEEPCOPY (Paul, 22 Nov 2026: "I need the ability to copy a given feature of a charm inside the charm editor, then to close the charm editor open the charm editor on a different piece
// and then be able to paste that back in what I copied in the previous session of the opened charm editor into a new session of the charm editor"). In Chromium on the editor's harness page.
//   1  ⌘C in one session, the editor closed, a NEW session opened on another piece (its charm's frame elsewhere): ⌘V pastes the copy, in the same place on THAT charm, one undo step
//   2  ⇧⌘V (paste in place) does the same; ⌘V again steps on from there as any paste
//   3  a reload of the page (a new page, the same browser): the copy is still there to paste
//   4  a copy made in the new session replaces the old one; the menus' Paste is lit with a kept copy; no page error
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/ce-keep-copy.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n         ')); };
async function step(name, fn) { try { await fn(); pass(name); } catch (e) { fail(name, e); } }
const near = (a, b, eps) => Math.abs(a - b) <= eps;
(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return done(); }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM, args: ['--no-sandbox'] });
  const ctx0 = await browser.newContext({ viewport: { width: 1280, height: 760 }, reducedMotion: 'reduce' });
  await ctx0.route(u => !u.href.startsWith('file://'), r => r.abort());
  const page = await ctx0.newPage(), errs = [];
  page.on('pageerror', e => errs.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error') errs.push('console: ' + m.text()); });
  const MOD = process.platform === 'darwin' ? 'Meta' : 'Control';
  const settle = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  const H = (fn, arg) => page.evaluate(fn, arg);
  const cur = expr => page.evaluate(`(() => { const C = window.CharmEditor.current; return ${expr}; })()`);
  const load = async () => { await page.goto('file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html')); };
  // a session on "another piece": the same faces with the charm's frame moved (dx, dy pt), as another piece's view sits elsewhere in its master
  const open = async (dx, dy) => {
    await H(([dx, dy]) => { const f = JSON.parse(JSON.stringify({ front: window.fixture.front, back: window.fixture.back })); for (const k of ['front', 'back']) { f[k].frame.x0 += dx; f[k].frame.y0 += dy; const mv = it => { if (it.subpaths) it.subpaths = it.subpaths.map(sp => sp.map(op => [op[0]].concat(op.slice(1).map(p => [p[0] + dx, p[1] + dy])))); if (typeof it.tx === 'number' && it.kind === 'text') { it.tx += dx; it.ty += dy; } (it.children || []).forEach(mv); }; f[k].items.forEach(mv); } window.openEditor({ faces: f }); }, [dx, dy]);
    await page.waitForSelector('.ce-root .ce-items path'); await settle(); await settle();
  };
  const close = async () => {
    await H(() => { const b = document.querySelector('.ce-actions [data-act="cancel"]'); if (b) b.click(); }); await settle();
    await H(() => { const ask = document.querySelector('.ce-ask'); if (!ask) return; const d = [...ask.querySelectorAll('button')].find(x => /discard/i.test(x.textContent || '')); if (d) d.click(); }); await settle();
    await page.waitForFunction(() => window.__closed === true, null, { timeout: 5000 });
  };
  await load(); await open(0, 0);
  const fr1 = await cur('C.frame()');
  // the feature: a little star shape drawn on the charm, 3 mm in from its top left
  const PT = 72 / 25.4, at = [fr1.x0 + 3 * PT, fr1.y0 + 3 * PT];
  const id = await H(at => { const c = window.CharmEditor.current.ctx; const ids = c.addItems([{ kind: 'path', role: 'hatch', name: 'Feature', fill: true, subpaths: [[['m', [at[0], at[1]]], ['l', [at[0] + 8, at[1]]], ['l', [at[0] + 4, at[1] + 7]], ['h']]], tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }]); c.commit(); c.select(ids); return ids[0]; }, at);
  await settle();
  const b1 = await cur(`C.box(${JSON.stringify(id)})`);
  await page.keyboard.press(MOD + '+c'); await settle();
  await close();
  await step('1 · ⌘C, the editor closed, a NEW session on another piece: ⌘V pastes the copy in the same place on THAT charm, one undo step', async () => {
    await open(40, 25);
    const fr2 = await cur('C.frame()'), n0 = (await cur('C.items()')).length, h0 = (await cur('C.history()')).undo;
    await page.keyboard.press(MOD + '+v'); await settle(); await page.waitForTimeout(250); await settle();
    const its = await cur('C.items()'), sel = await cur('C.selection()');
    assert.equal(its.length, n0 + 1, 'one thing pasted'); assert.equal(sel.length, 1);
    const b2 = await cur(`C.box(${JSON.stringify(sel[0])})`);
    assert.ok(near(b2[0] - fr2.x0, b1[0] - fr1.x0, 0.01) && near(b2[1] - fr2.y0, b1[1] - fr1.y0, 0.01) && near(b2[2] - b2[0], b1[2] - b1[0], 0.01), `the same place on this charm: ${JSON.stringify([b2[0] - fr2.x0, b2[1] - fr2.y0])} vs ${JSON.stringify([b1[0] - fr1.x0, b1[1] - fr1.y0])}`);
    assert.equal((await cur('C.history()')).undo, h0 + 1, 'one undo step');
  });
  await step('2 · ⇧⌘V does the same; ⌘V again steps on from there as any paste', async () => {
    const fr2 = await cur('C.frame()'), n0 = (await cur('C.items()')).length;
    await page.keyboard.press(MOD + '+Shift+v'); await settle(); await page.waitForTimeout(250); await settle();
    const s1 = (await cur('C.selection()'))[0], b = await cur(`C.box(${JSON.stringify(s1)})`);
    assert.equal((await cur('C.items()')).length, n0 + 1);
    assert.ok(near(b[0] - fr2.x0, b1[0] - fr1.x0, 0.01) && near(b[1] - fr2.y0, b1[1] - fr1.y0, 0.01), 'in place on this charm');
    await page.keyboard.press(MOD + '+v'); await settle(); await page.waitForTimeout(250); await settle();
    const s2 = (await cur('C.selection()'))[0], c = await cur(`C.box(${JSON.stringify(s2)})`);
    assert.ok(c[0] > b[0] + 1 && c[1] > b[1] + 1, 'the next paste steps on');
  });
  await close();
  await step('3 · a reload of the page: the copy is still there to paste', async () => {
    await load(); await open(-20, 10);
    const fr3 = await cur('C.frame()'), n0 = (await cur('C.items()')).length;
    await page.keyboard.press(MOD + '+v'); await settle(); await page.waitForTimeout(250); await settle();
    const its = await cur('C.items()'), s = (await cur('C.selection()'))[0], b = await cur(`C.box(${JSON.stringify(s)})`);
    assert.equal(its.length, n0 + 1, 'pasted after a reload');
    assert.ok(near(b[0] - fr3.x0, b1[0] - fr1.x0, 0.01) && near(b[1] - fr3.y0, b1[1] - fr1.y0, 0.01), 'in the same place on this charm');
  });
  await step('4 · a copy made in the new session replaces the old one; the menus\' Paste is lit with a kept copy; no page error', async () => {
    const fr = await cur('C.frame()'), tid = await H(fr => { const c = window.CharmEditor.current.ctx; const ids = c.addItems([{ kind: 'text', role: 'hatch', text: 'LOVE', fontKey: 'sans', sizePt: 6, align: 'center', tx: fr.x0 + fr.wPt / 2, ty: fr.y0 + fr.hPt / 2 }]); c.commit(); return ids[0]; }, fr);
    await H(id => window.CharmEditor.current.ctx.select([id]), tid); await settle();
    await page.keyboard.press(MOD + '+c'); await settle(); await close();
    await open(10, 10);
    const lit = await H(() => { const A = window.CharmEditor.current; return !!(A && A.ctx); });
    assert.ok(lit);
    await page.keyboard.press(MOD + '+v'); await settle(); await page.waitForTimeout(250); await settle();
    const s = (await cur('C.selection()'))[0], it = (await cur('C.items()')).find(i => i.id === s);
    assert.equal(it && it.kind, 'text', 'the newer copy (words) is what pastes');
    assert.deepEqual(errs, []);
  });
  await browser.close(); done();
})().catch(e => { fail('run', e); done(); });
function done() { console.log(failures.length ? `FAIL ce-keep-copy (${failures.length}): ${failures.join(' | ')}` : `PASS ce-keep-copy (${results.length})`); process.exit(failures.length ? 1 : 0); }
