// UPRIGHT2 (Paul, 29 Oct 2026, the bird SILHOUETTE 34560 of order 4211572095: "This charm flip to show Front/Back is incorrect. Charms must always be shown right side up and only mirroring the
// Back/Front in the Horizontal direction"). UPRIGHT made the two faces mirrors; the bird still came up upside down on both, because the hanging-hole rule (charm-nest-geom.js holeUpAngle) took the gap
// between its feet for the hole (up 267.5°) and he had to turn it himself (ROTBAR). The same angle sets the words' fit, so they would have been engraved upside down on the charm. Now the hole is the
// round ring in a loop at the edge (ringPick), a ring the master draws at its top keeps the master as drawn, and no ring found keeps it as drawn too.
//   D0 offline, the real bird (fixtures/PERCHED_43564.ai): the new rule finds the jump ring (round, at the top) and keeps the bird as drawn (up 90); the original rule (holeRule 1) still reads 267.5°
//   D1 the real sorter page over the fake site (_engraveRealWorld.cjs, offline, made-up names), the untouched bird engraved on its back ("T"): no person's turn; the back is drawn exactly as the master
//      (view turned 0°), the front is its left-right mirror, both right side up: the ring above the feet in both pictures
//   D2 the words are fitted level in that frame (fit angle 0)
//   D3 laser: a face decided before (the master's angle pinned at the old 267.5°, as an approved face's saved angle is) keeps that angle, and its approved back file is written; its normalised sha256
//      is printed (LASER ...) to compare with the same run on the code before (it must be identical); a saved face with no angle recorded is opened with the original rule (viewOptionsFor -> holeRule 1)
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/face-upright-default.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const crypto = require('crypto'), fs = require('fs'), path = require('path');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const Q = '#egQueue .rvItem[data-kind=placement]';
const OLD_UP = 267.50234443756403;   // what the original rule chose for the bird (the live library entry's upAngle)
const ENTRY = { sku: 'SILHOUETTE_34560', widthPt: 36.515260000000126, heightPt: 31.177639999999883, areaPt2: 449.3611111111111, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: OLD_UP, upSource: 'hole', charmHash: '831b2d80', backKeepOut: [] };

(async () => {
  const root = path.join(__dirname, '../..');
  const { CharmNestPDF: P, Geom: G } = require(path.join(root, 'netlify/functions/_charmNestPdf.js'));
  /* D0 · offline */
  {
    const parsed = await P.parseSource(fs.readFileSync(path.join(__dirname, 'fixtures', 'PERCHED_43564.ai')), 'PERCHED_43564.ai'), c = P.groupCharms(parsed).charms[0];
    P.integrateRings(c);
    const cut = Object.assign({}, c, { members: c.members.filter(m => m === c.outline || G.isCutLine(m)) });
    const now = G.upAngleOf(cut), was = G.upAngleOf(cut, { holeRule: 1 }), hb = now.hole && now.hole.bbox;
    // the ring: the round cut line in the loop at the top (19.4..23.1 × 25.9..29.6 pt); the feet's gap is the one at the bottom (y 7..11)
    check(`D0 the new rule takes the jump ring (${hb ? hb.map(v => v.toFixed(1)).join(',') : 'none'}) and keeps the bird as drawn: up ${now.angle} (${now.source}; the ring itself at ${now.holeAngle != null ? now.holeAngle.toFixed(1) : '?'}°)`,
      now.angle === 90 && now.source === 'hole' && hb && hb[1] > 20, JSON.stringify(now));
    check(`D0 the original rule (holeRule 1, kept for decisions saved with it) still reads the feet: up ${was.angle.toFixed(3)}°`, Math.abs(was.angle - OLD_UP) < 0.01 && was.hole && was.hole.bbox[3] < 15, JSON.stringify(was.angle));
    const vo = G.viewOptionsFor({ editingBack: true, savedUp: null, charmUp: null, entry: {} });
    check(`D3 a saved face with no angle recorded is opened with the original rule (${JSON.stringify(vo)}): up ${G.backView(c, Object.assign({}, vo)).upAngle.toFixed(3)}°`, vo.holeRule === 1 && Math.abs(G.backView(c, vo).upAngle - OLD_UP) < 0.01, '');
    const vt = G.viewOptionsFor({ turnUp: 123, editingBack: true, savedUp: OLD_UP });
    check('D3 a person\'s own turn (ROTBAR) still wins over every rule', vt.upAngle === 123 && vt.holeOnly === true, JSON.stringify(vt));
  }

  const R = require('./_engraveRealWorld.cjs');
  R.DESIGNS.SILHOUETTE_34560 = { file: 'PERCHED_43564.ai', entry: ENTRY };
  // (the same bird under a second name whose angle is pinned at the old rule's, as a face decided before keeps its saved angle: what the laser gets for it must not change)
  R.DESIGNS.SILHOUETTE_OLDUP = { file: 'PERCHED_43564.ai', entry: Object.assign({}, ENTRY, { sku: 'SILHOUETTE_OLDUP', upSource: 'operator' }) };
  R.ORDERS.sil = Object.assign({}, R.ORDERS.back, { rid: '4290000141', sku: 'SILHOUETTE_34560', listing: '1890000141', title: 'Perched Bird Silhouette Necklace in Sterling Silver', buyer: 'Name Twelve', personalization: ['T'] });
  R.ORDERS.silOld = Object.assign({}, R.ORDERS.back, { rid: '4290000142', sku: 'SILHOUETTE_OLDUP', listing: '1890000142', title: 'Perched Bird Silhouette Necklace in Sterling Silver', buyer: 'Name Thirteen', personalization: ['T'] });
  R.EXPECT_JOBS.sil = 1; R.EXPECT_JOBS.silOld = 1;
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: process.env.SHOTS || '/tmp/face-upright-default-shots' });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  await w.page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await w.page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } });
  const page = w.page, st = w.srv.st;
  try {
    const made = await R.build(w, ['sil', 'silOld']);
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    check('0 the world: the bird twice, each engraved on its back ("T"), fitted', ['sil', 'silOld'].every(n => made[n].jobs.length === 1 && made[n].jobs[0].face === 'back' && made[n].jobs[0].fitted), JSON.stringify([made.sil.jobs, made.silOld.jobs]));
    /** The faces of a piece as every picture draws them (CharmNestEngravePreview.frameFor) and where the ring and the feet land in each (y up: the ring above the feet = right side up). */
    const faces = pid => page.evaluate(pid => {
      const EP = CharmNestEngravePreview, G = CharmNestGeom, j = Engrave.jobsOfPiece(pid).back, c = Pool.charmOf(pid);
      const back = EP.frameFor({ job: j }), front = EP.frameFor({ face: 'front', row: j.row, slot: j.slot });
      const cut = c.members.filter(m => m !== c.outline && G.isCutLine(m)), ring = cut.find(m => m.bbox[1] > 20), feet = cut.find(m => m.bbox[3] < 15);
      const mid = m => [(m.bbox[0] + m.bbox[2]) / 2, (m.bbox[1] + m.bbox[3]) / 2];
      const ob = c.outline.bbox, cx = (ob[0] + ob[2]) / 2, cy = (ob[1] + ob[3]) / 2;
      const ap = (M, p) => [M[0] * p[0] + M[2] * p[1] + M[4], M[1] * p[0] + M[3] * p[1] + M[5]], mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
      const rot = up => { const t = (90 - up) * Math.PI / 180, k = Math.cos(t), s = Math.sin(t); return [k, s, -s, k, cx - cx * k + cy * s, cy - cx * s - cy * k]; };
      const toBack = mul(j.view.R, j.view.M), toFront = rot(front.up);
      return { up: j.view.upAngle, angleDeg: j.view.angleDeg, turnUp: j.turnUp == null ? null : j.turnUp, turn: Engrave.turnOf(j), frontUp: front.up, backKind: back && back.kind, frontKind: front && front.kind,
        toBack, toFront, ringB: ap(toBack, mid(ring)), feetB: ap(toBack, mid(feet)), ringF: ap(toFront, mid(ring)), feetF: ap(toFront, mid(feet)), fitAngle: j.fit && j.fit.angle, fitOk: !!(j.fit && j.verify && j.verify.geometry && j.verify.geometry.ok !== false) };
    }, pid);

    /* D1 · D2 */
    await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 900);
    const f = await faces(made.sil.poolIds[0]);
    const turnOf = m => { const det = m[0] * m[3] - m[1] * m[2], a = det < 0 ? -m[0] : m[0]; return Math.atan2(m[1], a) * 180 / Math.PI; };
    check(`D1 the untouched bird: no person's turn (turnUp ${f.turnUp}, box ${f.turn}°), the system's up ${f.up}° (was ${OLD_UP.toFixed(1)}°): the back drawn exactly as the master, turned ${f.angleDeg}°`,
      f.turnUp == null && f.turn === 0 && Math.abs(f.up - 90) < 1e-9 && Math.abs(f.angleDeg) < 1e-9, JSON.stringify(f));
    check(`D1 ... the back right side up: the ring (${f.ringB.map(v => v.toFixed(1))}) above the feet (${f.feetB.map(v => v.toFixed(1))})`, f.ringB[1] > f.feetB[1] + 10, JSON.stringify([f.ringB, f.feetB]));
    check(`D1 ... the front (up ${f.frontUp}°) its left-right mirror, right side up too: the ring (${f.ringF.map(v => v.toFixed(1))}) above the feet (${f.feetF.map(v => v.toFixed(1))}), neither picture turned (back ${turnOf(f.toBack).toFixed(2)}°, front ${turnOf(f.toFront).toFixed(2)}°)`,
      f.frontKind === 'charm' && Math.abs(f.frontUp - 90) < 1e-9 && f.ringF[1] > f.feetF[1] + 10 && Math.abs(turnOf(f.toBack)) < 1e-6 && Math.abs(turnOf(f.toFront)) < 1e-6 && Math.abs(f.ringB[1] - f.ringF[1]) < 1e-6, JSON.stringify(f));
    check(`D2 the words are fitted level in that upright frame (fit angle ${f.fitAngle}°, checked on the metal)`, f.fitAngle === 0 && f.fitOk, JSON.stringify([f.fitAngle, f.fitOk]));
    await R.shot(w, 'UP2-1-untouched-bird');

    /* D3 · the laser */
    const g = await faces(made.silOld.poolIds[0]);
    check(`D3 a face at a saved angle keeps it: up ${g.up.toFixed(3)}° (pinned at the old rule's)`, Math.abs(g.up - OLD_UP) < 1e-9, JSON.stringify(g.up));
    await R.openEngraveDetail(w, 'silOld', { face: 'back' }); await R.settle(page, 600);
    await page.click(Q + ' [data-a="approve"]', { timeout: 5000 });
    const t0 = Date.now(); let ai = null;
    while (Date.now() - t0 < 90000) { ai = [...st.blobs.keys()].find(k => /\/back\/.*\.ai$/.test(k)); if (ai) break; await new Promise(r => setTimeout(r, 300)); }
    let sha = '';
    if (ai) {
      const at = await page.evaluate(pid => String(Engrave.jobsOfPiece(pid).back.approvedAt || ''), made.silOld.poolIds[0]);
      // (the file's own dates and the approval's time are the run's, not the engraving's: taken out before the bytes are compared, as face-upright.cjs U5)
      const txt = st.blobs.get(ai).buf.toString('latin1').replace(/\/(CreationDate|ModDate) \([^)]*\)/g, '/$1 ()').split(at).join('AT').split([...at].map(ch => '00' + ch.charCodeAt(0).toString(16).toUpperCase()).join('')).join('AT').replace(/<xmp:(CreateDate|ModifyDate|MetadataDate)>[^<]*</g, '<xmp:$1><');
      sha = crypto.createHash('sha256').update(txt, 'latin1').digest('hex');
      if (process.env.LASER_OUT) fs.writeFileSync(process.env.LASER_OUT, txt, 'latin1');
    }
    console.log(`LASER back file ${ai || '(none)'} sha256 (dates out) ${sha}`);
    const after = await page.evaluate(pid => Engrave.jobsOfPiece(pid).back.view.upAngle, made.silOld.poolIds[0]);
    check(`D3 approved: the back file is written (${ai ? ai.split('/').pop() : 'none'}) at the saved angle (${after.toFixed(3)}°)`, !!ai && Math.abs(after - OLD_UP) < 1e-9, '');
    check('errors on the page: none', !w.errors.length, JSON.stringify(w.errors).slice(0, 400));
  } finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `FAILED ${bad.length} of ${RESULTS.length}: ${bad.map(r => r.id.slice(0, 40)).join(' | ')}` : `PASS ${RESULTS.length} checks: UPRIGHT2 — the untouched bird is shown right side up on both faces, its words level, a saved face's angle and file unchanged`);
  if (bad.length) process.exitCode = 1;
})().catch(e => { console.error(e); process.exit(1); });
