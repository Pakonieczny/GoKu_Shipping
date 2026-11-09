// CES3 (Paul, 9 Nov 2026): "I'm also not seeing the erase option in the left-hand menu inside the charm editor. I need to be able to erase portions of a vectorized image or any vectorized part
// of a charm inside the charm editor" ... "Make sure this option reflects the exact icon and functionality of Adobe illustrator" ... "they must be in the left-hand menu at the left side of the screen".
// On the real sorter page (_engraveRealWorld.cjs: offline), the Charm Editor opened on a new design (a 20 mm circle) with a filled square and a hatched "photo" group of greys:
//   R1  the Eraser is its own button in the Tools panel on the left (not in a flyout), right after the Scissors, named "Eraser Tool (Shift+E)"; Shift+E arms it too
//   R2  a drag across the square takes a bite out of it (its area smaller, still one part, the same id and role, curves kept: the bite's edge has curves)
//   R3  [ and ] change the size (kept for the next time), the circle under the pointer follows
//   R4  with the square selected, a drag across both the square and the photo erases only the square (Illustrator's rule); nothing selected, it erases the photo's greys too
//   R5  Alt-drag erases a rectangle; a drag across words says to Create Outlines first and leaves them; locked parts are never erased
//   R6  one ⌘Z gives each erase back whole; no page error
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ce-eraser.cjs
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const R = require('./_engraveRealWorld.cjs');

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, keepCards: true });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, settle = ms => R.settle(page, ms);
  try {
    await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* none */ } });
    // the editor on a new design: a circle outline (locked, as a cut), a filled square, a group of greys (a traced photo), and words
    await page.evaluate(() => {
      const PT = 72 / 25.4, M = CharmEditor.model, sq = (x, y, s) => [[['m', [x, y]], ['l', [x + s, y]], ['l', [x + s, y + s]], ['l', [x, y + s]], ['h']]];
      window.__ce = CharmEditor.open({ title: 'ERASE', faces: { front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 20 * PT, hPt: 20 * PT }, items: [
        { id: 'ring', kind: 'path', role: 'cut', name: 'Outline', subpaths: M.ellipseSP ? M.ellipseSP(10 * PT, 10 * PT, 9.5 * PT, 9.5 * PT) : sq(0.5 * PT, 0.5 * PT, 19 * PT), stroke: true, fill: false, locked: true },
        { id: 'sq', kind: 'path', role: 'hatch', name: 'Square', subpaths: sq(3 * PT, 3 * PT, 6 * PT), fill: true, stroke: false },
        { id: 'photo', kind: 'group', role: 'hatch', name: 'Image · photo', children: [
          { id: 'g1', kind: 'path', role: 'hatch', name: 'Grey 1', subpaths: sq(11 * PT, 3 * PT, 3 * PT), fill: [0.3, 0.3, 0.3], stroke: false },
          { id: 'g2', kind: 'path', role: 'hatch', name: 'Grey 2', subpaths: sq(14 * PT, 3 * PT, 3 * PT), fill: [0.6, 0.6, 0.6], stroke: false }] },
        { id: 'wd', kind: 'text', role: 'hatch', text: 'HI', fontKey: 'sans', sizePt: 10, align: 'center', tx: 10 * PT, ty: 15 * PT, sx: 1, sy: 1, rot: 0 }] } }, lockRoles: ['cut'] });
    });
    await page.waitForFunction(() => window.CharmEditor && CharmEditor.current && document.querySelector('.ce-root .ce-rail'));
    await settle(400);
    const PT = 72 / 25.4;
    const at = (x, y) => page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [x * PT, y * PT]);
    const area = id => page.evaluate(id => { const M = CharmEditor.model, ix = M.indexTree(CharmEditor.current.items(), CharmEditor.current.ctx.geo), e = ix.get(id); if (!e) return null; const it = e.it; const ops = window.CharmEditorOps; return { area: ops.kernel.filledArea(M.outlinesOf(it, CharmEditor.current.ctx.geo, e.P), !!it.evenOdd), role: it.role, curves: (it.subpaths || []).some(sp => sp.some(o => o[0] === 'c')) }; }, id);
    const drag = async (a, b, alt) => { const p = await at(a[0], a[1]), q = await at(b[0], b[1]); if (alt) await page.keyboard.down('Alt'); await page.mouse.move(p.x, p.y); await page.mouse.down(); for (let i = 1; i <= 8; i++) await page.mouse.move(p.x + (q.x - p.x) * i / 8, p.y + (q.y - p.y) * i / 8); await page.mouse.up(); if (alt) await page.keyboard.up('Alt'); await settle(250); };

    /* ── R1 ── */
    const r1 = await page.evaluate(() => {
      const b = document.querySelector('.ce-root .ce-rail > button.ce-tool[data-tool="eraser"]') || document.querySelector('.ce-root .ce-rail [data-tool="eraser"]');
      const inFly = !!(b && b.closest('.ce-menu')), prev = b && b.previousElementSibling;
      return { there: !!b, shown: !!(b && b.offsetParent && b.getBoundingClientRect().width > 10), inFly, label: b ? b.getAttribute('aria-label') : '', after: prev ? (prev.dataset.tool || (prev.querySelector('[data-tool]') || {}).dataset?.tool || '') : '', svg: !!(b && b.querySelector('svg')) };
    });
    await page.keyboard.press('Shift+E'); await settle(150);
    const armed = await page.evaluate(() => CharmEditor.current.tool());
    check(`R1 the Eraser is its own button on the left Tools panel (${r1.shown}, in no flyout: ${!r1.inFly}), after the ${r1.after}, "${r1.label}", its icon drawn; Shift+E arms it (${armed})`,
      r1.there && r1.shown && !r1.inFly && r1.svg && /Eraser Tool/.test(r1.label) && /Shift\+E|⇧E/.test(r1.label) && armed === 'eraser', JSON.stringify({ r1, armed }));

    /* ── R2 ── */
    const a0 = await area('sq');
    await drag([2, 6], [6, 6]);
    const a1 = await area('sq');
    check(`R2 a drag across the square takes a bite out of it (area ${a0 && a0.area.toFixed(2)} → ${a1 && a1.area.toFixed(2)} pt²), the same part (role ${a1 && a1.role}), the bite's edge curved (${a1 && a1.curves})`,
      !!a0 && !!a1 && a1.area < a0.area - 1 && a1.area > 0 && a1.role === 'hatch' && a1.curves, JSON.stringify({ a0, a1 }));

    /* ── R3 ── */
    const s0 = await page.evaluate(() => CharmEditorOps.eraseSize());
    await page.keyboard.press(']'); await page.keyboard.press(']');
    const s1 = await page.evaluate(() => CharmEditorOps.eraseSize());
    await page.keyboard.press('['); const s2 = await page.evaluate(() => CharmEditorOps.eraseSize());
    const p = await at(10, 10); await page.mouse.move(p.x, p.y); await settle(100);
    const circ = await page.evaluate(() => document.querySelectorAll('.ce-root svg circle').length);
    check(`R3 ] makes it bigger (${s0} → ${s1} mm), [ smaller (${s2} mm); the circle under the pointer is drawn (${circ})`, s1 > s0 && s2 < s1 && circ > 0, JSON.stringify({ s0, s1, s2, circ }));

    /* ── R4 ── */
    const g0 = await area('g1');
    await page.evaluate(() => CharmEditor.current.ctx.select(['sq']));
    const b0 = await area('sq');
    await drag([4, 7.5], [16, 7.5]);
    const b1 = await area('sq'), g1 = await area('g1');
    await page.evaluate(() => CharmEditor.current.ctx.select([]));
    await drag([10.5, 4.5], [18, 4.5]);
    const g2 = await area('g1'), h2 = await area('g2');
    check(`R4 with the square selected only it is erased (square ${b0.area.toFixed(1)} → ${b1.area.toFixed(1)}, grey ${g0.area.toFixed(1)} → ${g1.area.toFixed(1)}); nothing selected, the photo's greys are erased too (${g2 && g2.area.toFixed(1)}, ${h2 && h2.area.toFixed(1)})`,
      b1.area < b0.area - 1 && Math.abs(g1.area - g0.area) < 1e-6 && g2 && g2.area < g0.area - 1 && h2 && h2.area > 0, JSON.stringify({ b0, b1, g0, g1, g2, h2 }));

    /* ── R5 ── */
    const r0 = await area('g2');
    await drag([14.5, 3.5], [16.5, 5.5], true);
    const rr = await area('g2');
    const ring0 = await area('ring');
    await drag([10, 14], [10, 16.5]);
    const said = await page.evaluate(() => (document.querySelector('.ce-root .ce-say') || {}).textContent || '');
    const wd = await page.evaluate(() => !!CharmEditor.current.items().find(i => i.id === 'wd'));
    await drag([0, 10], [3, 10]);
    const ring1 = await area('ring');
    check(`R5 Alt-drag erases a rectangle (${r0.area.toFixed(1)} → ${rr.area.toFixed(1)} pt²); across words: "${said}", the words kept (${wd}); the locked outline never erased (${ring0 && ring0.area.toFixed(1)} → ${ring1 && ring1.area.toFixed(1)})`,
      rr.area < r0.area - 1 && /Create Outlines/.test(said) && wd && ring0 && ring1 && Math.abs(ring0.area - ring1.area) < 1e-6, JSON.stringify({ r0, rr, said, wd, ring0, ring1 }));

    /* ── R6 ── */
    const h0 = await page.evaluate(() => CharmEditor.current.history().undo);
    await page.keyboard.press('ControlOrMeta+z'); await settle(200);
    const back = await area('g2'), h1 = await page.evaluate(() => CharmEditor.current.history().undo);
    check(`R6 ⌘Z gives the last erase back whole (${rr.area.toFixed(1)} → ${back.area.toFixed(1)} pt², undo steps ${h0} → ${h1}); no page error`, Math.abs(back.area - r0.area) < 1e-3 && h1 === h0 - 1 && w.errors.length === 0, JSON.stringify({ back, h0, h1, errors: w.errors }));
    await page.evaluate(() => CharmEditor.current.ctx && document.querySelector('.ce-root [data-act="cancel"]').click()); await settle(200);
    await page.evaluate(() => { const y = document.querySelector('.ce-ask [data-ask="yes"]'); if (y) y.click(); });
  } catch (e) { check('the run', false, String(e && e.stack || e).slice(0, 600)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} passed${bad.length ? ' · FAILED: ' + bad.map(r => r.id.split(' ')[0]).join(', ') : ''}`);
  process.exit(bad.length ? 1 : 0);
})();
