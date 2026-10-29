// SHAREDSHAPE (Paul, 29 Oct 2026, the back of a watering-can charm: "Now you made it so I can't unlock the charm layers ... the user must always be able to unlock all the layers of any charm. The back
// and front views must adapt simultaneously, so if a user is changing anything on the back of you that has a cut out or a rotation or anything that would affect also the front view in the front
// view must naturally adapt an exact same way in real time since it's the same charm it's like the same coin with two different sides"). The charm's SHAPE (its cut parts) is ONE for both faces in
// the Charm Editor (FaceDoc.toCharm), kept by the FRONT record whichever face it was changed on; a placed piece's cut line changes too, its sheet checked again before it is written.
// On the real bird (fixtures/PERCHED_43564.ai, SILHOUETTE_34560) opened from its BACK card, on a sheet:
//   S0 both faces say how they stand on the charm: the back's cut lines, mirrored onto the front, ARE the front's
//   S1 the whole charm turned 90° on the back (Unlock All, ⌘A, Rotate 90°): the front's cut lines turned the same way, mirrored, at once (no face switch), and the front's engraving turned with them
//      (one coin); S2 ⌘Z: both faces as they were
//   S3 a cut-out added on the back: on the front at once, the same part, mirrored
//   S4 Save from the back: saved, no refusal; the FRONT record holds the cut-out (role cut), the back's none; the piece's charm (what the sheet, the laser, the previews and the work area read)
//      has it; the sheet was checked again where the piece stands (the sheet window's check) and written, the card says so
//   S5 a cut line changed on the placed piece where it would overlap (the sheet's check says no): Save asks first ("The cut line changed", Save anyway); saved anyway, the sheet is NOT
//      written, held (cutCheck) until the check passes again
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ce-shared-shape.cjs      (SHOTS=<dir> for screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const Q = '#egQueue .rvItem[data-kind=placement]';
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
// (node side) the affine map through point pairs, and the farthest miss
function fitMap(pairs) {
  let sxx = 0, sxy = 0, syy = 0, sx = 0, sy = 0, n = 0; const bx = [0, 0, 0], by = [0, 0, 0];
  for (const [a, b] of pairs) { const x = a[0], y = a[1]; sxx += x * x; sxy += x * y; syy += y * y; sx += x; sy += y; n++; bx[0] += x * b[0]; bx[1] += y * b[0]; bx[2] += b[0]; by[0] += x * b[1]; by[1] += y * b[1]; by[2] += b[1]; }
  const A = [[sxx, sxy, sx], [sxy, syy, sy], [sx, sy, n]];
  const solve = v => { const m = A.map((r, i) => r.concat(v[i])); for (let c = 0; c < 3; c++) { let pv = c; for (let r = c + 1; r < 3; r++) if (Math.abs(m[r][c]) > Math.abs(m[pv][c])) pv = r; [m[c], m[pv]] = [m[pv], m[c]]; for (let r = 0; r < 3; r++) if (r !== c) { const f = m[r][c] / m[c][c]; for (let k = c; k < 4; k++) m[r][k] -= f * m[c][k]; } } return [m[0][3] / m[0][0], m[1][3] / m[1][1], m[2][3] / m[2][2]]; };
  const X = solve(bx), Y = solve(by), T = [X[0], Y[0], X[1], Y[1], X[2], Y[2]];
  let miss = 0; for (const [a, b] of pairs) { const q = [T[0] * a[0] + T[2] * a[1] + T[4], T[1] * a[0] + T[3] * a[1] + T[5]]; miss = Math.max(miss, Math.hypot(q[0] - b[0], q[1] - b[1])); }
  return { T, miss };
}
const pts = subs => { const out = []; for (const sp of subs || []) for (const o of sp) for (let k = 1; k < o.length; k++) out.push(o[k]); return out; };
const far = (a, b) => { const p = pts(a), q = pts(b); if (p.length !== q.length) return Infinity; let m = 0; p.forEach((x, i) => { m = Math.max(m, Math.hypot(x[0] - q[i][0], x[1] - q[i][1])); }); return m; };

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  R.DESIGNS.SILHOUETTE_34560 = { file: 'PERCHED_43564.ai', entry: { sku: 'SILHOUETTE_34560', widthPt: 36.515260000000126, heightPt: 31.177639999999883, areaPt2: 449.3611111111111, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: 267.50234443756403, upSource: 'hole', charmHash: '831b2d80', backKeepOut: [] } };
  R.ORDERS.sil = Object.assign({}, R.ORDERS.back, { rid: '4290000141', sku: 'SILHOUETTE_34560', listing: '1890000141', title: 'Perched Bird Silhouette Necklace in Sterling Silver', buyer: 'Name Twelve', personalization: ['Charm Maker'] });
  R.EXPECT_JOBS.sil = 1;
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: process.env.SHOTS || '/tmp/ce-shared-shape-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  await w.page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await w.page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } });
  const page = w.page, st = w.srv.st;
  try {
    const made = await R.build(w, ['sil']);
    const pid = made.sil.poolIds[0];
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    // (the sheet as written: its files are written again after a save, and every write is counted)
    await page.evaluate(pid => { const sh = Pool.sheetOf(pid); sh.outputs = sh.outputs || { ai: 'test' }; window.__writes = 0; window.writeAsPlaced = async () => { window.__writes++; return true; }; }, pid);
    const open = async () => { await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 700); await page.$eval(Q + ' .egCharmEdit', b => b.click()); await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page); };
    await open();
    /** Each face's parts as drawn (face points), and how its points stand on the charm. */
    const shape = () => page.evaluate(() => {
      const E = CharmEditor.current, geo = E.ctx.geo, M = CharmEditor.model, r = E.result(), out = {};
      for (const f of ['front', 'back']) out[f] = { toCharm: r.faces[f].toCharm, parts: E.items(f).filter(i => i.kind !== 'group').map(i => ({ id: i.id, role: i.role, locked: !!i.locked, subs: M.transformSubpaths(geo.raw(i).subpaths, geo.matrix(i)) })) };
      return out;
    });
    const M = { mul: (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]],
      inv: m => { const d = m[0] * m[3] - m[1] * m[2], a = m[3] / d, b = -m[1] / d, c = -m[2] / d, e = m[0] / d; return [a, b, c, e, -(a * m[4] + c * m[5]), -(b * m[4] + e * m[5])]; },
      map: (m, subs) => subs.map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(p => [m[0] * p[0] + m[2] * p[1] + m[4], m[1] * p[0] + m[3] * p[1] + m[5]]))))) };
    /** The farthest the back's cut parts, put onto the front, are from the front's own (pt), and the ids each face has. */
    const same = s => { const BF = M.mul(M.inv(s.front.toCharm), s.back.toCharm); let m = 0; const bc = s.back.parts.filter(p => p.role === 'cut'), fc = s.front.parts.filter(p => p.role === 'cut');
      for (const b of bc) { const f = fc.find(x => x.id === b.id); m = Math.max(m, f ? far(M.map(BF, b.subs), f.subs) : Infinity); } return { miss: m, back: bc.map(p => p.id).sort().join(), front: fc.map(p => p.id).sort().join() }; };
    const cmd = name => page.evaluate(n => { const c = CharmEditor.commands[n]; if (!c) return 'no ' + n; c.run(CharmEditor.current.ctx); return 'ok'; }, name);
    const stage = async () => { const r = await page.evaluate(() => { const b = document.querySelector('.ce-root .ce-stage').getBoundingClientRect(); return [b.left, b.top]; }); await page.mouse.click(r[0] + 8, r[1] + 8); };

    /* S0 */
    const s0 = await shape(), m0 = same(s0);
    check(`S0 both faces say how they stand on the charm, and the back's cut lines (${m0.back}) put onto the front ARE the front's (${m0.front}): ${m0.miss.toFixed(4)} pt`, Array.isArray(s0.front.toCharm) && Array.isArray(s0.back.toCharm) && m0.back === m0.front && m0.miss < 0.01, JSON.stringify(m0));
    check(`S0 the cut lines open locked on the back (${s0.back.parts.filter(p => p.role === 'cut').map(p => p.locked).join()})`, s0.back.parts.filter(p => p.role === 'cut').every(p => p.locked), '');

    /* S1 · the whole charm turned on the back */
    await page.evaluate(() => CharmEditor.current.ai.act('unlockAll')); await settleP(page);
    const u = await page.evaluate(() => ({ dirty: CharmEditor.current.dirty(), locked: CharmEditor.current.items('back').filter(i => i.locked).length }));
    check(`S1 Unlock All on the back: every lock off (${u.locked} left), and nothing changed on either face (dirty ${u.dirty})`, u.locked === 0 && u.dirty === false, JSON.stringify(u));
    await page.keyboard.press('v'); await stage(); await page.keyboard.press('ControlOrMeta+a'); await settleP(page);
    const selN = (await page.evaluate(() => CharmEditor.current.selection())).length, ran = await cmd('ops.rot90'); await settleP(page);
    const faceNow = await page.evaluate(() => CharmEditor.current.face());
    const s1 = await shape(), m1 = same(s1);
    const fc0 = s0.front.parts.filter(p => p.role === 'cut'), fc1 = s1.front.parts.filter(p => p.role === 'cut');
    const pairs = []; for (const a of fc0) { const b = fc1.find(x => x.id === a.id), p = pts(a.subs), q = pts(b.subs); p.forEach((x, i) => pairs.push([x, q[i]])); }
    const T = fitMap(pairs).T, turned = Math.abs(T[0]) < 1e-3 && Math.abs(Math.abs(T[1]) - 1) < 1e-3 && Math.abs(T[1] + T[2]) < 1e-3;
    const art0 = s0.front.parts.filter(p => p.role !== 'cut'), art1 = s1.front.parts.filter(p => p.role !== 'cut');
    let artMiss = 0; for (const a of art0) { const b = art1.find(x => x.id === a.id); artMiss = Math.max(artMiss, b ? far(M.map(T, a.subs), b.subs) : Infinity); }
    check(`S1 the whole charm turned 90° on the back (${selN} parts, ${ran}), still on the ${faceNow}: the front's cut lines are the back's, mirrored (${m1.miss.toFixed(4)} pt), turned a quarter (${T.slice(0, 4).map(v => v.toFixed(3)).join(' ')})`,
      ran === 'ok' && faceNow === 'back' && m1.miss < 0.01 && turned && m1.back === m0.back, JSON.stringify({ m1, T }));
    check(`S1 ... and the front's engraving (${art0.length} parts: ${art0.map(p => p.role).join(', ')}) turned with them, as one coin (${artMiss.toFixed(4)} pt)`, art0.length >= 1 && artMiss < 0.02, String(artMiss));
    await R.shot(w, 'shape-S1-back-turned');
    /* S2 · undo */
    await page.keyboard.press('ControlOrMeta+z'); await settleP(page);
    const s2 = await shape(); let back2 = 0, front2 = 0;
    for (const a of s0.front.parts) { const b = s2.front.parts.find(x => x.id === a.id); front2 = Math.max(front2, b ? far(a.subs, b.subs) : Infinity); }
    for (const a of s0.back.parts) { const b = s2.back.parts.find(x => x.id === a.id); back2 = Math.max(back2, b ? far(a.subs, b.subs) : Infinity); }
    check(`S2 ⌘Z: both faces as they were (front ${front2.toFixed(4)} pt, back ${back2.toFixed(4)} pt)`, front2 < 1e-6 && back2 < 1e-6, JSON.stringify({ front2, back2 }));

    /* S3 · a cut-out on the back */
    const holeId = await page.evaluate(() => {
      const E = CharmEditor.current, b = E.box(E.items().find(i => i.role === 'hatch' || i.kind === 'text').id), cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2, r = 0.6, k = 0.5523 * r;
      const circle = [[['m', [cx + r, cy]], ['c', [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ['c', [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ['c', [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ['c', [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]], ['h']]];
      const ids = E.ctx.addItems([{ kind: 'path', role: 'cut', name: 'Hole', stroke: [0, 0, 0], fill: null, subpaths: circle }]); E.ctx.commit('hole'); return ids[0];
    });
    await settleP(page);
    const s3 = await shape(), m3 = same(s3);
    check(`S3 a cut-out added on the back (${holeId}): on the front at once, the same part mirrored (${m3.miss.toFixed(4)} pt)`, !!holeId && m3.front.split(',').includes(holeId) && m3.miss < 0.01, JSON.stringify(m3));
    await R.shot(w, 'shape-S3-hole');

    /* S4 · Save from the back */
    const puts0 = st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'artPut').length, writes0 = await page.evaluate(() => window.__writes);
    await page.click('.ce-root [data-act="save"]');
    const t0 = Date.now(); let out = null;
    while (Date.now() - t0 < 90000) { out = await page.evaluate(() => ({ open: !!document.querySelector('.ce-root'), refuse: (document.querySelector('.ce-root .ce-refuse') || {}).textContent || '' })); if (!out.open || out.refuse) break; await new Promise(r => setTimeout(r, 250)); }
    const t1 = Date.now(); let note = '';
    while (Date.now() - t1 < 30000) { note = await page.evaluate(() => (document.querySelector('#egQueue .rvItem[data-kind=placement] .egArtNote') || {}).textContent || ''); if (/saved/i.test(note) && !/Saving/.test(note)) break; await new Promise(r => setTimeout(r, 250)); }
    await page.evaluate(() => new Promise(r => setTimeout(r, 600)));
    const front = st.doc('Charm_Pool_Art', pid + '~front'), backRec = st.doc('Charm_Pool_Art', pid), puts = st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'artPut').length - puts0;
    const pc = await page.evaluate(({ pid, holeId }) => { const c = Pool.charmOf(pid), j = [...Engrave.items().values()].find(j => (j.copies || []).includes(pid)); return { has: !!(c && c.members.some(m => m.artMemberId === holeId)), cut: c && (c.members.find(m => m.artMemberId === holeId) || {}).artRole, view: !!(j && j.view && (j.view.members || []).some(m => m.artMemberId === holeId)), held: !!(Pool.sheetOf(pid) || {}).cutCheck, writes: window.__writes }; }, { pid, holeId });
    await R.shot(w, 'shape-S4-saved');
    check(`S4 Save from the back: saved (${puts} save${puts === 1 ? '' : 's'}), no refusal, the editor closed`, out && !out.open && !out.refuse && puts >= 1, JSON.stringify(out));
    check(`S4 the FRONT record holds the cut-out (role cut, the record says cut), the back's none (${backRec ? JSON.stringify(backRec.added.map(a => a.role)) : 'no back record'})`,
      front && front.cut === true && (front.added || []).some(a => a.id === holeId && a.role === 'cut') && (!backRec || !(backRec.added || []).some(a => a.role === 'cut')), JSON.stringify(front || null).slice(0, 300));
    check(`S4 the piece's charm has the cut-out (${pc.cut}), and the back's work area draws it (${pc.view})`, pc.has && pc.cut === 'cut' && pc.view, JSON.stringify(pc));
    check(`S4 the sheet was checked again where the piece stands and written (${pc.writes - writes0} write, held ${pc.held}); the card says so: "${note.slice(0, 200)}"`, !pc.held && pc.writes > writes0 && /checked again and it still fits/.test(note), JSON.stringify({ pc, note }));

    /* S5 · a cut line that would overlap */
    await page.evaluate(() => { window.__fits = CNManual.fitsSpot; CNManual.fitsSpot = () => ({ ok: false, why: 'overlap', reason: "No room there: it would overlap order 4290000999's charm." }); });
    const writes5 = await page.evaluate(() => window.__writes);
    await open();
    await page.evaluate(() => CharmEditor.current.ai.act('unlockAll')); await settleP(page);
    await page.evaluate(id => CharmEditor.current.ctx.select([id]), holeId); await page.keyboard.press('Shift+ArrowRight'); await settleP(page);
    await page.click('.ce-root [data-act="save"]');
    const ask = await page.waitForSelector('.ce-root .ce-refuse [data-ask="go"]', { timeout: 60000 }).then(() => page.evaluate(() => (document.querySelector('.ce-root .ce-refuse') || {}).textContent || '')).catch(() => '');
    await R.shot(w, 'shape-S5-asked');
    check(`S5 a cut line changed where it would overlap: Save asks first, "${ask.slice(0, 220)}"`, /The cut line changed/.test(ask) && /would overlap/.test(ask) && /Save anyway/.test(ask), ask);
    const n5 = st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'artPut').length;
    await page.click('.ce-root .ce-refuse [data-ask="go"]');
    const t5 = Date.now(); while (Date.now() - t5 < 60000 && (await page.evaluate(() => !!document.querySelector('.ce-root')) || st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'artPut').length === n5)) await new Promise(r => setTimeout(r, 250));
    await page.evaluate(() => new Promise(r => setTimeout(r, 800)));
    const h5 = await page.evaluate(pid => { const sh = Pool.sheetOf(pid); return { held: !!(sh.cutCheck && sh.cutCheck[pid]), why: sh.cutCheck && sh.cutCheck[pid] && sh.cutCheck[pid].why, writes: window.__writes, note: (document.querySelector('#egQueue .rvItem[data-kind=placement] .egArtNote') || {}).textContent || '', still: window.cutHeld(sh) }; }, pid);
    check(`S5 saved anyway: the sheet is NOT written (${h5.writes - writes5} writes) and is held (${h5.why}); the card says so: "${h5.note.slice(0, 200)}"`, h5.held && h5.still && h5.writes === writes5 && /keeps its old files/.test(h5.note), JSON.stringify(h5));
    const h6 = await page.evaluate(pid => { CNManual.fitsSpot = window.__fits; const sh = Pool.sheetOf(pid); return { still: window.cutHeld(sh), left: !!sh.cutCheck }; }, pid);
    check('S5 ... and once its check passes again (moved clear), the sheet is no longer held', !h6.still && !h6.left, JSON.stringify(h6));
    check('errors on the page: none', !w.errors.length, JSON.stringify(w.errors).slice(0, 400));
  } finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `FAILED ${bad.length} of ${RESULTS.length}: ${bad.map(r => r.id).join(', ')}` : `PASS ${RESULTS.length} checks: SHAREDSHAPE — one shape for both faces in real time (a turn, a cut-out), undo of both, the front record keeps it, a placed piece's sheet checked again and never written unchecked`);
  if (bad.length) process.exitCode = 1;
})().catch(e => { console.error(e); process.exit(1); });
