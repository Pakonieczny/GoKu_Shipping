// STEADYGRID (Paul, 28 Oct 2026: "When I rotate the entire charm, the grid gets resized, and due to that resizing it goes behind the button on the top, left hand corner … and then those buttons
// that are attached to the grid are than hidden behind the button also just like the grid. The grid should be static and only the charm inside the grid should rotate. The grid should also be always
// in the same place and also be the same full size and then the user should be able to manipulate the charm inside the grid").
// The real sorter page over the repo's fake site (_engraveRealWorld.cjs; offline, made-up names), on two real masters read from the live site (fixtures/PERCHED_43564.ai, the bird SILHOUETTE_34560,
// engraved on its back; fixtures/TINY_BAR_7427.ai, the bar, engraved on its front), at 1440×900, 1280×800 and 1024×768:
//   1  the work area's grid (its canvas) has the same size and place at every turn of the whole piece (0°, 35°, 90°, 180°, with the card's own turn), and for the other face (Front | Back)
//   2  the turned piece stays wholly inside the grid at every turn (every cut line's box and the words' box with its handle), and the mm grid is the same millimetres (one scale)
//   3  the grid, its "1 mm grid" label and its Front | Back switch are never under the "Open in Charm Editor" button nor the Approve block
//   4  the words can still be dragged with the piece turned, and Approve (a real mouse press) still approves
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-work-grid.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const Q = '#egQueue .rvItem[data-kind=placement]';
const VIEWS = [[1440, 900], [1280, 800], [1024, 768]];

function seedWorld(R) {
  R.DESIGNS.SILHOUETTE_34560 = { file: 'PERCHED_43564.ai', entry: { sku: 'SILHOUETTE_34560', widthPt: 36.515260000000126, heightPt: 31.177639999999883, areaPt2: 449.3611111111111, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: 267.50234443756403, upSource: 'hole', charmHash: '831b2d80', backKeepOut: [] } };
  R.DESIGNS['TINY BAR_7427'] = { file: 'TINY_BAR_7427.ai', entry: { sku: 'TINY BAR_7427', sym: 'symmetric', widthPt: 50.956999999999994, heightPt: 11.25, areaPt2: 420.6388888888889, members: 1, holes: 0, engravable: true, engravableBy: 'index', upAngle: 89.852, upSource: 'long', charmHash: '9ac1e42a', backKeepOut: [] } };
  R.ORDERS.sil = Object.assign({}, R.ORDERS.back, { rid: '4290000141', sku: 'SILHOUETTE_34560', listing: '1890000141', title: 'Perched Bird Silhouette Necklace in Sterling Silver', buyer: 'Name Twelve', personalization: ['T'] });
  R.ORDERS.bar = { rid: '4290000151', sku: 'TINY BAR_7427', listing: '1890000151', title: 'Tiny Bar Necklace, Personalized Coordinates Bar Necklace in Sterling Silver', buyer: 'Name Thirteen', placed: { day: 0, h: 10, m: 20 }, ship: 5,
    variations: [{ name: 'Metal choice', value: 'Silver' }, { name: 'Chain length', value: '16"' }, { name: 'Font', value: 'Angelina' }], personalization: ['33.47765N 86.80020W'] };
  R.EXPECT_JOBS.sil = 1; R.EXPECT_JOBS.bar = 1;
}

(async () => {
  const R = require('./_engraveRealWorld.cjs'); seedWorld(R);
  const shots = process.env.SHOTS || '/tmp/engrave-work-grid-shots';
  const w = await R.world({ viewport: { width: VIEWS[0][0], height: VIEWS[0][1] }, shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  await w.page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await w.page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step on this page */ } });   // (the "Laser or Design?" bar covers buttons)
  const page = w.page, st = w.srv.st;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
  /** The work area now: the grid's rect, the piece and the words in it (canvas px), the controls and what they must stay clear of. */
  const look = () => page.evaluate(Q => {
    const c = document.querySelector(Q), cv = c && c.querySelector('.backHost canvas'), j = c && Engrave.items().get(c.dataset.key); if (!cv || !j) return null;
    const rect = el => { if (!el) return null; const r = el.getBoundingClientRect(); return r.width && r.height ? [r.left, r.top, r.right, r.bottom].map(v => Math.round(v * 10) / 10) : null; };
    const s = cv.getBoundingClientRect(), sc = s.width / cv.width, tx = cv._map.tx, v = cv._viewFace ? null : j.view;
    // the piece as drawn: its cut lines' boxes (the view the canvas draws) and the words' box and handle, in screen px
    const members = (cv._viewFace ? [] : v.members).map(m => { const b = m.bbox, p = [tx(b[0], b[1]), tx(b[2], b[3])]; return [s.left + Math.min(p[0][0], p[1][0]) * sc, s.top + Math.min(p[0][1], p[1][1]) * sc, s.left + Math.max(p[0][0], p[1][0]) * sc, s.top + Math.max(p[0][1], p[1][1]) * sc]; });
    const words = cv._box ? cv._box.corners.concat([cv._box.rotate]).map(p => [s.left + p[0] * sc, s.top + p[1] * sc]) : [];
    const g = cv._gridLabel, label = g ? [s.left + g.x * sc, s.top + g.y * sc, s.left + (g.x + g.w) * sc, s.top + (g.y + g.h) * sc] : null;
    return { grid: rect(cv), k: Math.round(cv._map.k * 1e6) / 1e6, steady: !!cv._steady, members, words, label, sw: rect(c.querySelector('.backHost .egFaceView')), ce: rect(c.querySelector('.egCharmEdit')), ap: rect(c.querySelector('.pvApproval')),
      apBtn: rect(c.querySelector('[data-a="approve"], [data-a="approved"]')), turn: Engrave.turnOf(j), face: cv._viewFace || (j.face === 'front' ? 'front' : 'back'), key: j.key, state: j.state, centre: j.fit && j.fit.centre.slice() };
  }, Q);
  const over = (a, b) => !!(a && b && a[0] < b[2] - 0.5 && b[0] < a[2] - 0.5 && a[1] < b[3] - 0.5 && b[1] < a[3] - 0.5);
  const inside = (a, g) => !!(a && g && a[0] >= g[0] - 0.6 && a[1] >= g[1] - 0.6 && a[2] <= g[2] + 0.6 && a[3] <= g[3] + 0.6);
  const ptIn = (p, g) => p[0] >= g[0] - 0.6 && p[1] >= g[1] - 0.6 && p[0] <= g[2] + 0.6 && p[1] <= g[3] + 0.6;
  const same = (a, b) => !!(a && b && a.every((v, i) => Math.abs(v - b[i]) <= 0.5));
  const turn = d => page.evaluate(([Q, d]) => { const c = document.querySelector(Q), j = Engrave.items().get(c.dataset.key), o = Engrave.turnTo(j, d); Engrave.repaintTurn(j); return o; }, [Q, d]);
  try {
    const made = await R.build(w, ['sil', 'bar']);
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    check(`0 the world: the bird engraved on its back, the bar on its front, both fitted`, made.sil.jobs.length === 1 && made.sil.jobs[0].fitted && made.bar.jobs.length === 1 && made.bar.jobs[0].fitted, JSON.stringify({ sil: made.sil.jobs, bar: made.bar.jobs }));
    for (const [vw, vh] of VIEWS) {
      await page.setViewportSize({ width: vw, height: vh }); await R.settle(page, 400);
      for (const [name, face] of [['sil', 'back'], ['bar', 'front']]) {
        const at = `${name} ${vw}×${vh}`;
        await R.openEngraveDetail(w, name, { face }); await R.settle(page, 900);
        await turn(0); await R.settle(page, 300);
        const seen = [];
        for (const d of [0, 35, 90, 180]) { const o = await turn(d); await R.settle(page, 350); seen.push(Object.assign(await look(), { d, ok: o.ok || o.same })); }
        const g0 = seen[0].grid, wide = await page.evaluate(Q => !!document.querySelector(Q + ' .pvMain.egWide'), Q);
        check(`1 ${at}: the grid stays put and the same size at 0°, 35°, 90° and 180° (${g0 && g0.map(Math.round).join(',')}${wide ? ', the wide work area' : ''}), one scale (${seen[0].k} px a pt)`,
          seen.every(s => s.ok && s.steady && same(s.grid, g0) && s.k === seen[0].k) && seen.map(s => s.turn).join() === '0,35,90,180', JSON.stringify(seen.map(s => [s.d, s.turn, s.grid, s.k, s.ok])));
        check(`2 ${at}: the turned piece is wholly inside the grid at every turn (its cut lines, and the words' box with its turning handle)`,
          seen.every(s => s.members.length && s.members.every(m => inside(m, s.grid)) && s.words.every(p => ptIn(p, s.grid))), JSON.stringify(seen.map(s => [s.d, s.members.filter(m => !inside(m, s.grid)), s.words.filter(p => !ptIn(p, s.grid))])));
        check(`3 ${at}: the grid, its "1 mm grid" label and its Front | Back switch are clear of the "Open in Charm Editor" button and the Approve block at every turn`,
          seen.every(s => !!s.ce && !over(s.grid, s.ce) && !over(s.grid, s.ap) && !over(s.label, s.ce) && !over(s.sw, s.ce) && !over(s.sw, s.ap) && !over(s.label, s.ap) && (!s.sw || inside(s.sw, s.grid))),
          JSON.stringify(seen.map(s => ({ d: s.d, grid: s.grid, label: s.label, sw: s.sw, ce: s.ce, ap: s.ap }))));
        await R.shot(w, `GRID-${name}-${vw}x${vh}-180`);
        // the other face (the bird's front, the bar's back: no job of their own), through the switch: the very same grid
        if (seen[0].sw) {
          const other = face === 'back' ? 'front' : 'back';
          await page.click(Q + ` .backHost .egFaceView [data-fv="${other}"]`); await R.settle(page, 400);
          const o = await look();
          await page.click(Q + ` .backHost .egFaceView [data-fv="${face}"]`); await R.settle(page, 400);
          check(`1b ${at}: the ${other} (the switch) is drawn in the same grid, the same size and place`, o.face === other && same(o.grid, g0) && o.k === seen[0].k, JSON.stringify({ o: [o.face, o.grid, o.k], g0 }));
        }
        await turn(0); await R.settle(page, 300);
      }
      await R.shot(w, `GRID-${vw}x${vh}`);
    }
    // 4 · the words dragged with the bird turned 35°, then one real press on Approve
    await page.setViewportSize({ width: 1440, height: 900 }); await R.settle(page, 300);
    await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 900);
    await turn(35); await R.settle(page, 400);
    const d0 = await look(), cv0 = await page.evaluate(Q => { const cv = document.querySelector(Q + ' .backHost canvas'), r = cv.getBoundingClientRect(), s = r.width / cv.width; return { x: r.left + cv._box.centrePx[0] * s, y: r.top + cv._box.centrePx[1] * s }; }, Q);
    await page.mouse.move(cv0.x, cv0.y); await page.mouse.down(); await page.mouse.move(cv0.x + 5, cv0.y + 4, { steps: 4 }); await page.mouse.up(); await R.settle(page, 500);
    const d1 = await look();
    check(`4 the bird turned 35°: the words are dragged in the steady grid (their centre moved), and the grid did not move`, !!d0.centre && !!d1.centre && (d1.centre[0] !== d0.centre[0] || d1.centre[1] !== d0.centre[1]) && same(d1.grid, d0.grid), JSON.stringify({ d0: d0.centre, d1: d1.centre, g: [d0.grid, d1.grid] }));
    await page.click(Q + ' [data-a="approve"]', { timeout: 5000 });
    const ap = await until(() => page.evaluate(k => { const j = Engrave.items().get(k); return j && /approved|written/.test(j.state) ? j.state : null; }, d1.key), 60000);
    check(`4b one real press on Approve approves the turned bird (${ap})`, !!ap, String(ap));
    check('E1 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 600));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
