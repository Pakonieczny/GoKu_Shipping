// CURVETEXT (Paul, 22 Nov 2026: "add the ability to curve the text so that it matches the circumference of the circle ... a simple, quick drag tool like illustrator has to easily
// adjust any text so that it matches the form and circumference of a given circle disc"). No server, no network.
//   N1 a text bent with arcR stands on its circle: every letter of its first line between the circle and a capital outside it, the words centred on the top; a smile (arcR < 0) under
//      its centre; arcR gone, the words are straight again exactly as before
//   N2 two lines (an initial over a date) bend on two circles of ONE centre, the second a line's distance in
//   N3 a saved design keeps arcR / arcX (charm-nest-designs.js clean), so it opens again curved
//   B  in the editor (Chromium): the Curve text tool is on the rail; a press on a text and a drag up arches it; dragged near the disc's own curve it snaps to fit (concentric with the
//      disc, "Fits the disc"); one undo step; Fit to the disc and Straighten on the bar; its circle drawn while selected
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/ce-curve-text.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const CE = require(path.join(root, 'charm-nest-charm-editor.js')), X = require(path.join(root, 'charm-nest-ce-extras.js'));
const M = CE.model, PT = 72 / 25.4, MM = 25.4 / 72, geo = M.createGeo();
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n         ')); };
function check(name, fn) { try { fn(); pass(name); } catch (e) { fail(name, e); } }
async function step(name, fn) { try { await fn(); pass(name); } catch (e) { fail(name, e); } }
const near = (a, b, eps) => Math.abs(a - b) <= eps;
const ptsOf = sps => { const o = []; for (const sp of sps) for (const op of sp) for (const q of op.slice(1)) if (Array.isArray(q)) o.push(q); return o; };
const layout = it => X.textLayout.layout(it, geo.engine);
const flatOf = it => { const r = geo.engine(it.text, it.fontKey, it.sizePt, { align: it.align }); return r.subpaths; };

console.log('ce-curve-text · N (node)');
check('N1 a text arched with arcR stands on its circle (baseline on it, letters outward), centred on its top; a smile goes under; arcR gone, straight as before', () => {
  const t = { kind: 'text', role: 'hatch', text: '08.15.23', fontKey: 'sans', sizePt: 7, align: 'center' };
  const flat = M.bboxOf(flatOf(t)), xm = (flat[0] + flat[2]) / 2, R = 4 * PT, cap = X.capOf(geo.engine, 'sans', 7), hw = 0.46 * 7 * 0.7 / 6;
  const a = layout(Object.assign({}, t, { arcR: R })); assert.ok(a && a.subpaths.length, 'laid out');
  const C = [xm, R], rr = ptsOf(a.subpaths).map(q => Math.hypot(q[0] - C[0], q[1] - C[1]));
  assert.ok(Math.min(...rr) > R - 2 * hw - 0.05 && Math.max(...rr) < R + cap + 2 * hw + 0.05, `radii ${Math.min(...rr).toFixed(2)}..${Math.max(...rr).toFixed(2)} for R ${R.toFixed(2)} cap ${cap.toFixed(2)}`);
  const b = M.bboxOf(a.subpaths); assert.ok(near((b[0] + b[2]) / 2, xm, 0.2), 'centred: ' + ((b[0] + b[2]) / 2 - xm));
  assert.ok(b[3] - b[1] > (flat[3] - flat[1]) + 1, 'it bends: its box is taller than the straight words');
  const s = layout(Object.assign({}, t, { arcR: -R })), C2 = [xm, -R], r2 = ptsOf(s.subpaths).map(q => Math.hypot(q[0] - C2[0], q[1] - C2[1]));
  assert.ok(Math.min(...r2) > R - cap - 2 * hw - 0.05 && Math.max(...r2) < R + 2 * hw + 0.05, 'a smile: the letters inside the circle above them, their baseline on it');
  assert.equal(X.textLayout.key(t), '', 'no arcR: the engine\'s own words'); assert.ok(X.textLayout.bent(Object.assign({}, t, { arcR: R })), 'bent: a click finds the letters');
  console.log(`       08.15.23 on a ${(R * MM).toFixed(1)} mm circle: radii ${(Math.min(...rr) * MM).toFixed(2)}–${(Math.max(...rr) * MM).toFixed(2)} mm`);
});
check('N2 an initial over a date bends on two circles of one centre, the date a line\'s distance in', () => {
  const t = { kind: 'text', role: 'hatch', text: 'A\n08.15.23', fontKey: 'sans', sizePt: 7, align: 'center', arcR: 6 * PT, arcX: 0 };
  const a = layout(t), two = geo.engine('H\nH', 'sans', 7, { align: 'left' }), one = geo.engine('H', 'sans', 7, { align: 'left' }), lead = M.bboxOf(two.subpaths)[3] - M.bboxOf(one.subpaths)[3];
  const date = layout(Object.assign({}, t, { text: '08.15.23' })), cap = X.capOf(geo.engine, 'sans', 7), hw = 0.46 * 7 * 0.7 / 6, R = 6 * PT;
  // (the date alone on the first line sits on R; as the second line it sits on R − lead, same centre)
  const C = [0, R], rOf = sps => ptsOf(sps).map(q => Math.hypot(q[0] - C[0], q[1] - C[1]));
  const all = rOf(a.subpaths), lo = Math.min(...all), hi = Math.max(...all);
  assert.ok(lo > R - lead - 2 * hw - 0.05 && hi < R + cap + 2 * hw + 0.05, `two lines between ${(R - lead).toFixed(2)} and ${(R + cap).toFixed(2)}: ${lo.toFixed(2)}..${hi.toFixed(2)}`);
  assert.ok(Math.max(...rOf(date.subpaths)) > R + cap * 0.8, 'the date on the first line reaches R + cap');
});
check('N3 a saved design keeps arcR and arcX (it opens again curved)', () => {
  const D = require(path.join(root, 'charm-nest-designs.js'));
  const src = require('fs').readFileSync(path.join(root, 'charm-nest-designs.js'), 'utf8');
  assert.ok(/"arcR", "arcX"\]\);/.test(src) && /\["pathAt", "pathRisePt", "tracking", "arcR", "arcX"\]/.test(src), 'KNOWN and the number checks name them');
  assert.ok(D, 'loads');
});

/* ════ B · the editor in Chromium ════ */
(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return done(); }
  console.log('ce-curve-text · B (Chromium)');
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM, args: ['--no-sandbox'] });
  const ctx0 = await browser.newContext({ viewport: { width: 1280, height: 760 }, reducedMotion: 'reduce' });
  await ctx0.route(u => !u.href.startsWith('file://'), r => r.abort());
  const page = await ctx0.newPage(), errs = [];
  page.on('pageerror', e => errs.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error') errs.push('console: ' + m.text()); });
  await page.goto('file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html'));
  await page.addScriptTag({ path: path.join(root, 'charm-nest-vector.js') });
  await page.addScriptTag({ path: path.join(root, 'charm-nest-ce-extras.js') });
  const settle = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  const H = (fn, arg) => page.evaluate(fn, arg);
  const cur = expr => page.evaluate(`(() => { const C = window.CharmEditor.current; return ${expr}; })()`);
  const item = id => H(id => { let f = null; const w = l => (l || []).forEach(i => { if (i.id === id) f = i; w(i.children); }); w(window.CharmEditor.current.items()); return f; }, id);
  const clientOf = (x, y) => H(([x, y]) => window.CharmEditor.current.clientOf(x, y), [x, y]);
  await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle(); await settle();
  const fr = await cur('C.frame()'), cx = fr.x0 + fr.wPt / 2, cy = fr.y0 + fr.hPt / 2;
  const disc = await H(() => { const c = window.CharmEditor.current.ctx; let b = null; for (const it of c.items()) if (it.role === 'cut') { const e = c.entry(it.id), bb = c.geo.drawn(e.it, e.P); if (!b || (bb[2] - bb[0]) > (b[2] - b[0])) b = bb; } return b; });
  const dc = [(disc[0] + disc[2]) / 2, (disc[1] + disc[3]) / 2];
  const ids = await H(([x, y]) => window.CharmEditor.current.ctx.addItems([{ kind: 'text', role: 'hatch', text: '08.15.23', fontKey: 'sans', sizePt: 6, align: 'center', tx: x, ty: y }]), [dc[0], dc[1] - 3 * 72 / 25.4]);
  await H(() => window.CharmEditor.current.ctx.commit()); await settle();
  const id = ids[0];
  await step('B1 the Curve text tool is on the rail', async () => {
    assert.ok(await H(() => !!document.querySelector('[data-tool="curve-text"]')), 'its rail button');
  });
  await step('B2 a press on the words and a drag up arches them (one undo step); the size chip says it', async () => {
    const h0 = (await cur('C.history()')).undo;
    await H(() => document.querySelector('[data-tool="curve-text"]').click()); await settle();
    const b = await cur(`C.box(${JSON.stringify(id)})`), a = await clientOf((b[0] + b[2]) / 2, (b[1] + b[3]) / 2);
    await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(a.x, a.y - 12, { steps: 6 }); await settle();
    await page.mouse.up(); await settle();
    const t = await item(id); assert.ok(t.arcR > 0, 'arched: arcR ' + t.arcR);
    assert.equal((await cur('C.history()')).undo, h0 + 1, 'one undo step');
    assert.ok(await H(() => !!document.querySelector('.ce-ui .cx-guide, .cx-guide')), 'its circle drawn while selected');
  });
  await step('B3 dragged near the disc\'s own curve it snaps to fit: concentric with the disc', async () => {
    const t0 = await item(id), want = await H(([id, dc]) => { const c = window.CharmEditor.current.ctx, e = c.entry(id), W = c.CharmEditor ? null : null; const M = window.CharmEditor.model, Wm = M.mul(e.P || M.I, c.geo.matrix(e.it)), Wi = M.inv(Wm); return M.ap(Wi, dc[0], dc[1]); }, [id, dc]);
    // the drag that gives the disc's own radius: from the straight words, the sagitta of the fit over their width
    await H(id => window.CharmEditor.current.ctx.replaceItems({ [id]: Object.assign({}, window.CharmEditor.current.ctx.entry(id).it, { arcR: undefined, arcX: undefined }) }), id); await H(() => window.CharmEditor.current.ctx.commit()); await settle();
    const b = await cur(`C.box(${JSON.stringify(id)})`), wPt = b[2] - b[0], R = want[1], h = wPt * wPt / (8 * R);   // (the tool's drag for a bend 1/R)
    if (!(Number.isFinite(h) && h > 0)) throw new Error(`no fit drag: R ${R} w ${wPt} h ${h} want ${JSON.stringify(want)} box ${JSON.stringify(b)}`);
    const a = await clientOf((b[0] + b[2]) / 2, (b[1] + b[3]) / 2), z = await cur('C.zoom()') / 100;
    const a2 = await clientOf((b[0] + b[2]) / 2, (b[1] + b[3]) / 2 - h * 1.05);
    await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(a2.x, a2.y, { steps: 8 }); await settle();
    const chip = await H(() => (document.querySelector('.ce-live, .ce-chip, [data-live]') || {}).textContent || '');
    await page.mouse.up(); await settle();
    const t = await item(id);
    assert.ok(near(t.arcR, R, 1e-3) && near(t.arcX, want[0], 1e-3), `snapped to the disc: arcR ${t.arcR} vs ${R.toFixed(4)}, arcX ${t.arcX} vs ${want[0].toFixed(4)} (chip: ${chip})`);
  });
  await step('B4 Straighten and Fit to the disc on the bar', async () => {
    await H(() => window.CharmEditor.current.setTool ? window.CharmEditor.current.setTool('select') : null); await settle();
    const btn = id2 => H(k => { const b = document.querySelector(`[data-xcmd="${k}"], [data-cmd="${k}"]`); if (!b) return false; b.click(); return true; }, id2);
    const ranOff = await btn('cx.arcOff') || await H(() => { window.CharmEditor.commands['cx.arcOff'].run(window.CharmEditor.current.ctx); return 'run'; });
    await settle(); let t = await item(id); assert.ok(!(t.arcR), 'straight: ' + t.arcR + ' (' + ranOff + ')');
    const ranFit = await btn('cx.arcFit') || await H(() => { window.CharmEditor.commands['cx.arcFit'].run(window.CharmEditor.current.ctx); return 'run'; });
    await settle(); t = await item(id); assert.ok(t.arcR > 0, 'fitted: ' + t.arcR + ' (' + ranFit + ')');
  });
  await step('B5 no page error', async () => { assert.deepEqual(errs, []); });
  await browser.close();
  done();
})().catch(e => { fail('B', e); done(); });
function done() { console.log(failures.length ? `FAIL ce-curve-text (${failures.length}): ${failures.join(' | ')}` : `PASS ce-curve-text (${results.length})`); process.exit(failures.length ? 1 : 0); }
