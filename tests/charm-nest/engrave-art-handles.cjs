// ARTHANDLES (CES3, Paul, 9 Nov 2026): "If I add an image in the charm editor, then I should be able to click that image in the engraving modal and rotate, resize it, position it just like I
// would with regular text engraving" ... "These features and options already exist in the charm editor, so why can't you just add them to the engraving modal".
// On the real sorter page over the repo's fake site (_engraveRealWorld.cjs: offline, made-up names, nothing real written), an engraved necklace:
//   H1  a square "image" (a group of two greys, as the image importer makes them) added in the Charm Editor from the engraving card and saved
//   H2  in the engraving card's work area a click on it selects it: its box with four corners and a turning handle, drawn as the words' are
//   H3  dragged: saved for the piece (one more artwork save), the Charm Editor opened next has it where it was let go (moved by the drag, within 0.2 mm)
//   H4  a corner dragged outwards: bigger (saved, the editor agrees); H5 the turning handle dragged a quarter round: turned 90° (saved, the editor agrees)
//   H6  a press on the words still moves the words (their own handles), not the image; no page error
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-art-handles.cjs
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const R = require('./_engraveRealWorld.cjs');
const PT = 72 / 25.4;

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page, st = w.srv.st, settle = ms => R.settle(page, ms);
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
  const Q = '#egQueue .rvItem[data-kind=placement]';
  const artPuts = () => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === 'artPut').length;
  try {
    await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
    await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* none */ } });
    const made = await R.build(w, ['back']);
    const openCard = async () => { await R.openEngraveDetail(w, 'back'); await settle(900); };
    const openEditor = async () => { await page.$eval(Q + ' .egCharmEdit', b => b.click()); await page.waitForFunction(() => window.CharmEditor && CharmEditor.current && CharmEditor.current.items().length, null, { timeout: 60000 }); await settle(300); };
    const closeEditor = async () => { await page.evaluate(() => document.querySelector('.ce-root [data-act="cancel"]').click()); await until(() => page.evaluate(() => !(window.CharmEditor && CharmEditor.current)), 15000); await settle(400); };
    /** the image as the editor has it now: its box (pt, the editor's frame) */
    let IDS = [];
    const inEditor = () => page.evaluate(ids => { const E = CharmEditor.current, M = CharmEditor.model, geo = E.ctx.geo, ix = M.indexTree(E.items(), geo); let b = null; for (const [, e] of ix) { if (e.it.kind === 'group' || !ids.includes(String(e.it.id))) continue; const q = geo.drawn(e.it, e.P); if (q) b = b ? [Math.min(b[0], q[0]), Math.min(b[1], q[1]), Math.max(b[2], q[2]), Math.max(b[3], q[3])] : q; } return b; }, IDS);

    /* ── H1 ── */
    await openCard(); await openEditor();
    const sq0 = await page.evaluate(() => {
      const E = CharmEditor.current, f = E.frame(), s = Math.min(f.wPt, f.hPt) * 0.16, x = f.x0 + f.wPt * 0.5 - s / 2, y = f.y0 + f.hPt * 0.62 - s / 2;
      const sq = (x, y, s) => [[['m', [x, y]], ['l', [x + s, y]], ['l', [x + s, y + s]], ['l', [x, y + s]], ['h']]];
      E.ctx.addItems([{ kind: 'group', role: 'hatch', name: 'Image · photo', children: [
        { kind: 'path', role: 'hatch', name: 'Grey 1', fill: [0.3, 0.3, 0.3], stroke: false, artGrey: true, even: true, evenOdd: true, subpaths: sq(x, y, s), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 },
        { kind: 'path', role: 'hatch', name: 'Grey 2', fill: [0.6, 0.6, 0.6], stroke: false, artGrey: true, even: true, evenOdd: true, subpaths: sq(x + s * 0.25, y + s * 0.25, s * 0.5), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }] }], { select: false });
      E.ctx.commit('image'); return [x, y, x + s, y + s];
    });
    const p0 = artPuts();
    await page.click('.ce-root [data-act="save"]');
    await until(() => artPuts() > p0 && page.evaluate(() => !document.querySelector('.ce-root')), 90000);
    await until(() => page.evaluate(() => !document.querySelector('.cem-card') && !document.querySelector('dialog.cpDlg[open]')), 20000);
    await openCard();
    check(`H1 an image added in the Charm Editor and saved (${artPuts() - p0} artwork save)`, artPuts() > p0, '');

    /* the image's middle and corners on screen, from the card's work area */
    const onScreen = box => page.evaluate(([Q, b]) => { const cv = document.querySelector(Q + ' .backHost canvas'); if (!cv || !cv._map) return null; const r = cv.getBoundingClientRect(), k = r.width / cv.width, at = (x, y) => { const p = cv._map.tx(x, -y); return { x: r.left + p[0] * k, y: r.top + p[1] * k }; }; return { c: at((b[0] + b[2]) / 2, (b[1] + b[3]) / 2), br: at(b[2], b[3]), k: cv._map.k * k }; }, [Q, box]);
    const selNow = () => page.evaluate(Q => { const cv = document.querySelector(Q + ' .backHost canvas'); return cv && cv._artSel ? cv._artSel() : null; }, Q);
    /* ── H2 ── */
    let s0 = await onScreen(sq0);
    await page.mouse.move(s0.c.x, s0.c.y); await page.mouse.down(); await page.mouse.up(); await settle(200);
    const picked = await selNow(); IDS = picked || [];
    check(`H2 a click on the image in the engraving card selects it (${JSON.stringify(picked)}), its box and handles drawn`, Array.isArray(picked) && picked.length === 2, JSON.stringify(picked));

    /* ── H3 · move ── */
    const p1 = artPuts(), dxMm = 1.5;
    s0 = await onScreen(sq0);
    await page.mouse.move(s0.c.x, s0.c.y); await page.mouse.down();
    for (let i = 1; i <= 6; i++) await page.mouse.move(s0.c.x + dxMm * PT * s0.k * i / 6, s0.c.y);
    await page.mouse.up();
    await until(() => artPuts() > p1, 30000); await settle(1500);
    await openCard(); await openEditor(); const b3 = await inEditor(); await closeEditor();
    const moved = b3 ? ((b3[0] + b3[2]) / 2 - (sq0[0] + sq0[2]) / 2) / PT : null;
    check(`H3 dragged ${dxMm} mm in the engraving card: saved (${artPuts() - p1}), and the Charm Editor has it moved ${moved != null ? moved.toFixed(2) : '-'} mm`, artPuts() > p1 && moved != null && Math.abs(moved - dxMm) < 0.2, JSON.stringify({ b3, sq0 }));

    /* ── H4 · resize ── */
    await openCard();
    const w3 = b3[2] - b3[0];
    let s1 = await onScreen(b3);
    await page.mouse.move(s1.c.x, s1.c.y); await page.mouse.down(); await page.mouse.up(); await settle(200);   // (selected: its corners are live)
    s1 = await onScreen(b3);
    const p2 = artPuts(), pad = 1.2 * PT * s1.k, from = { x: s1.br.x + pad, y: s1.br.y + pad };
    await page.mouse.move(from.x, from.y); await page.mouse.down();
    for (let i = 1; i <= 6; i++) await page.mouse.move(from.x + (from.x - s1.c.x) * 0.5 * i / 6, from.y + (from.y - s1.c.y) * 0.5 * i / 6);
    await page.mouse.up();
    await until(() => artPuts() > p2, 30000); await settle(1500);
    await openCard(); await openEditor(); const b4 = await inEditor(); await closeEditor();
    const grow = b4 ? (b4[2] - b4[0]) / w3 : null;
    check(`H4 a corner dragged out: saved (${artPuts() - p2}), the Charm Editor has it ${grow ? grow.toFixed(2) : '-'}× as wide`, artPuts() > p2 && grow > 1.3, JSON.stringify({ b3, b4 }));

    /* ── H5 · turn ── */
    await openCard();
    let s2 = await onScreen(b4);
    await page.mouse.move(s2.c.x, s2.c.y); await page.mouse.down(); await page.mouse.up(); await settle(200);
    const hp = await page.evaluate(([Q, b]) => { const cv = document.querySelector(Q + ' .backHost canvas'), r = cv.getBoundingClientRect(), k = r.width / cv.width, at = (x, y) => { const p = cv._map.tx(x, -y); return { x: r.left + p[0] * k, y: r.top + p[1] * k }; }; return at((b[0] + b[2]) / 2, b[1] - 1.2 * 72 / 25.4 - 2.4 * 72 / 25.4); }, [Q, b4]);
    s2 = await onScreen(b4);
    const rr = Math.hypot(hp.x - s2.c.x, hp.y - s2.c.y), p3 = artPuts();
    await page.mouse.move(hp.x, hp.y); await page.mouse.down();
    for (let i = 1; i <= 10; i++) { const a = -Math.PI / 2 + (Math.PI / 2) * i / 10; await page.mouse.move(s2.c.x + rr * Math.cos(a), s2.c.y + rr * Math.sin(a)); }
    await page.mouse.up();
    await until(() => artPuts() > p3, 30000); await settle(1500);
    await openCard(); await openEditor();
    const turned = await page.evaluate(ids => { const E = CharmEditor.current, M = CharmEditor.model, geo = E.ctx.geo, ix = M.indexTree(E.items(), geo); const out = []; for (const [, e] of ix) { if (!ids.includes(String(e.it.id))) continue; const sp = M.outlinesOf(e.it, geo, e.P)[0]; const a = sp[0][1], b = sp[1][1]; out.push(Math.round(Math.atan2(b[1] - a[1], b[0] - a[0]) * 180 / Math.PI)); } return out.length ? out[out.length - 1] : null; }, IDS);
    await closeEditor();
    check(`H5 the turning handle dragged a quarter round: saved (${artPuts() - p3}), the image's top edge now runs at ${turned}° in the Charm Editor`, artPuts() > p3 && turned != null && Math.abs(Math.abs(turned) - 90) <= 2, JSON.stringify({ turned }));

    /* ── H6 · the words keep their own handles ── */
    await openCard();
    const words = await page.evaluate(Q => { const cv = document.querySelector(Q + ' .backHost canvas'); if (!cv || !cv._box) return null; const r = cv.getBoundingClientRect(), k = r.width / cv.width; return { x: r.left + cv._box.centrePx[0] * k, y: r.top + cv._box.centrePx[1] * k }; }, Q);
    const c0 = await page.evaluate(Q => { const j = Engrave.items().get(document.querySelector(Q).dataset.key); return j.fit ? j.fit.centre.slice() : null; }, Q);
    const p4 = artPuts();
    if (words) { await page.mouse.move(words.x, words.y); await page.mouse.down(); for (let i = 1; i <= 5; i++) await page.mouse.move(words.x, words.y - 20 * i / 5); await page.mouse.up(); }
    await settle(1500);
    const c1 = await page.evaluate(Q => { const j = Engrave.items().get(document.querySelector(Q).dataset.key); return j.fit ? j.fit.centre.slice() : null; }, Q);
    check(`H6 a drag on the words moves the words (${JSON.stringify(c0)} → ${JSON.stringify(c1)}), no artwork save for it (${artPuts() - p4}); no page error`, !!words && !!c0 && !!c1 && Math.abs(c1[1] - c0[1]) > 0.5 && artPuts() === p4 && w.errors.length === 0, JSON.stringify({ words, c0, c1, errors: w.errors }));
  } catch (e) { check('the run', false, String(e && e.stack || e).slice(0, 700)); }
  finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} passed${bad.length ? ' · FAILED: ' + bad.map(r => r.id.split(' ')[0]).join(', ') : ''}`);
  process.exit(bad.length ? 1 : 0);
})();
