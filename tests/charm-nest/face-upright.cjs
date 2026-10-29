// UPRIGHT (Paul, 29 Oct 2026, the Design & Engrave card of the bird SILHOUETTE 34560, order 4211572095: "This charm flip to show Front/Back is incorrect. Charms must always be shown right side up
// and only mirroring the Back/Front in the Horizontal direction"). The back he had turned right side up stood beside a front the system still turned by a rule of its own: the same bird upside down.
// A face with nothing fitted on it is now drawn as the exact left-right mirror of the other face as that face is drawn (charm-nest-geom.js pairedUp), in the card's thumbnails (and every picture made
// by CharmNestEngravePreview.frameFor: list rows, made thumbs, the order window), the work area's other face and the Charm Editor's faces (charm-nest-bridge.js charmEditView). Display only.
// The real sorter page over the repo's fake site (_engraveRealWorld.cjs; offline, made-up names) on the real bird master (fixtures/PERCHED_43564.ai), engraved on its BACK ("T"), on a sheet:
//   U1 as the system turned it: the Front box is the Back box mirrored left-right (the transform between them a pure horizontal mirror: det -1, no turn, no vertical flip; the cut lines' boxes match)
//   U2 the piece placed turned 90° and 180° on its sheet: every face picture exactly as at 0° (the sheet's turn never reaches a face)
//   U3 the Charm Editor's two faces are each other's left-right mirror (FaceDoc.toCharm), and the work area's Front (the switch) draws
//   U4 Paul's case: the back turned 180° (right side up): the Front box, the work area's Front and the Charm Editor's front follow it, mirrored, right side up (within the system's own hole angle)
//   U5 nothing the laser reads changes: the job's view, mask and fit are untouched by every picture above; approved, its back file is written (its normalised sha256 printed for a before/after run)
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/face-upright.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const crypto = require('crypto');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const Q = '#egQueue .rvItem[data-kind=placement]';
const settleP = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
// (node side) 2×3 affine maps [a, b, c, d, e, f]: x' = a x + c y + e, y' = b x + d y + f
const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
const inv = m => { const d = m[0] * m[3] - m[1] * m[2], a = m[3] / d, b = -m[1] / d, c = -m[2] / d, e = m[0] / d; return [a, b, c, e, -(a * m[4] + c * m[5]), -(b * m[4] + e * m[5])]; };
/** A pure left-right mirror (x' = k - x, y' = y): the determinant -1, no turn, no vertical flip, no move up or down. */
const hMirror = m => Math.abs(m[0] + 1) < 1e-6 && Math.abs(m[1]) < 1e-6 && Math.abs(m[2]) < 1e-6 && Math.abs(m[3] - 1) < 1e-6 && Math.abs(m[5]) < 1e-6;
const fmt = m => m.map(v => (Math.abs(v) < 5e-10 ? 0 : v).toFixed(4)).join(' ');
/** The turn of a map from the charm's own points, degrees (a mirror read through its mirror). */
const turnOf = m => { const det = m[0] * m[3] - m[1] * m[2], a = det < 0 ? -m[0] : m[0], b = m[1]; return Math.atan2(b, a) * 180 / Math.PI; };

(async () => {
  const R = require('./_engraveRealWorld.cjs');
  R.DESIGNS.SILHOUETTE_34560 = { file: 'PERCHED_43564.ai', entry: { sku: 'SILHOUETTE_34560', widthPt: 36.515260000000126, heightPt: 31.177639999999883, areaPt2: 449.3611111111111, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: 267.50234443756403, upSource: 'hole', charmHash: '831b2d80', backKeepOut: [] } };
  R.ORDERS.sil = Object.assign({}, R.ORDERS.back, { rid: '4290000141', sku: 'SILHOUETTE_34560', listing: '1890000141', title: 'Perched Bird Silhouette Necklace in Sterling Silver', buyer: 'Name Twelve', personalization: ['T'] });
  R.EXPECT_JOBS.sil = 1;
  const shots = process.env.SHOTS || '/tmp/face-upright-shots';
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  await w.page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await w.page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } });
  const page = w.page, st = w.srv.st;
  try {
    const made = await R.build(w, ['sil']);
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    const pid = made.sil.poolIds[0];
    check(`0 the world: the bird engraved on its back ("T"), fitted, nothing on its front`, made.sil.jobs.length === 1 && made.sil.jobs[0].face === 'back' && made.sil.jobs[0].fitted, JSON.stringify(made.sil.jobs));

    /** Every face picture of the piece now: the Back / Front boxes' frames (CharmNestEngravePreview.frameFor, as the card, the list rows, the made thumbs and the order window draw them), as maps
     *  from the charm's own points, and their cut lines' boxes; the job's view, mask and fit as the laser reads them (a fingerprint). */
    const faces = () => page.evaluate(pid => {
      const EP = CharmNestEngravePreview, j = Engrave.jobsOfPiece(pid).back, row = j.row;
      const back = EP.frameFor({ job: j }), front = EP.frameFor({ face: 'front', row, slot: j.slot });
      const ob = Pool.charmOf(pid).outline.bbox, cx = (ob[0] + ob[2]) / 2, cy = (ob[1] + ob[3]) / 2;
      const boxes = F => (F ? F.cut.map(m => m.bbox.slice()) : null);
      // (a frame that does not say its turn, as before UPRIGHT: the front's own rule, so this test also runs on the code before it for the laser's before/after)
      if (front && front.up == null) { const c = Pool.charmOf(pid), cut = c.members.filter(m => m === c.outline || CharmNestGeom.isCutLine(m)); front.up = CharmNestGeom.upAngleOf(Object.assign({}, c, { members: cut }), { face: 'front' }).angle; }
      const v = j.view, fp = JSON.stringify([v.M, v.R, v.angleDeg, v.upAngle, j.mask && [j.mask.w, j.mask.h, j.mask.ox, j.mask.oy, Array.prototype.join.call(j.mask.bits, '')], j.fit && [j.fit.centre, j.fit.angle, j.fit.size, j.fit.cmds]]);
      return { cx, cy, back: { kind: back && back.kind, M: v.M, R: v.R, up: v.upAngle, boxes: boxes(back) }, front: { kind: front && front.kind, up: front && front.up, boxes: boxes(front),
        R: front && front.up != null ? (() => { const t = (90 - front.up) * Math.PI / 180, c = Math.cos(t), s = Math.sin(t); return [c, s, -s, c, cx - cx * c + cy * s, cy - cx * s - cy * c]; })() : null }, fp, turn: Engrave.turnOf(j) };
    }, pid);
    /** The map from the Back picture to the Front picture, and how far the Front's cut-line boxes are from the Back's mirrored about the charm's centre line. */
    const relate = f => {
      const toBack = mul(f.back.R, f.back.M), toFront = f.front.R, BF = mul(toFront, inv(toBack));
      let miss = Infinity;
      if (f.front.boxes && f.back.boxes && f.front.boxes.length === f.back.boxes.length) { miss = 0; const k = BF[4]; f.back.boxes.forEach((b, i) => { const m = [k - b[2], b[1], k - b[0], b[3]], q = f.front.boxes[i]; miss = Math.max(miss, ...m.map((v, n) => Math.abs(v - q[n]))); }); }
      return { BF, miss, frontTurn: turnOf(toFront), backTurn: turnOf(toBack) };
    };

    await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 900);
    /* U1 */
    const f0 = await faces(), r0 = relate(f0);
    check(`U1 as the system turned it (back up ${f0.back.up.toFixed(3)}°): the Front box (up ${f0.front.up != null ? f0.front.up.toFixed(3) : '?'}°) is the Back box mirrored left-right: ${fmt(r0.BF)}`, f0.back.kind === 'view' && f0.front.kind === 'charm' && hMirror(r0.BF), JSON.stringify({ f0: [f0.back.up, f0.front.up], BF: r0.BF }));
    check(`U1 ... the Front's cut lines are the Back's mirrored about the charm's centre line (${r0.miss.toFixed(4)} pt)`, r0.miss < 0.02, String(r0.miss));
    await R.shot(w, 'UP-1-system');

    /* U2 · the piece placed turned on its sheet */
    const turned = [];
    for (const a of [90, 180]) {
      await page.evaluate(([pid, a]) => { const sh = Pool.sheetOf(pid), p = (sh.placements || []).find(x => x.id === pid || String(x.id).endsWith(pid)) || (sh.placements || [])[0]; if (p) p.angle = a; }, [pid, a]);
      await page.evaluate(() => { Engrave.render(); }); await R.settle(page, 500);
      const f = await faces(); turned.push({ a, same: JSON.stringify([f.back, f.front]) === JSON.stringify([f0.back, f0.front]), mirror: hMirror(relate(f).BF), ang: await page.evaluate(pid => ((Pool.sheetOf(pid).placements || [])[0] || {}).angle, pid) });
    }
    check(`U2 the piece placed turned 90° and 180° on its sheet: every face picture exactly as at 0° (${turned.map(t => `${t.ang}°: ${t.same ? 'same' : 'CHANGED'}`).join(', ')})`, turned.every(t => t.same && t.mirror && (t.ang === 90 || t.ang === 180)), JSON.stringify(turned));
    await page.evaluate(pid => { const sh = Pool.sheetOf(pid); for (const p of sh.placements || []) p.angle = 0; Engrave.render(); }, pid); await R.settle(page, 500);

    /** The Charm Editor opened from the card: the map from its Back face to its Front face (FaceDoc.toCharm, as ce-shared-shape.cjs reads it), then put away with nothing changed. */
    const editor = async name => {
      await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 600);
      await page.$eval(Q + ' .egCharmEdit', b => b.click());
      await page.waitForSelector('.ce-root .ce-items path', { timeout: 60000 }); await settleP(page); await settleP(page);
      const r = await page.evaluate(() => { const x = CharmEditor.current.result(); return { front: x.faces.front.toCharm, back: x.faces.back.toCharm }; });
      await R.shot(w, name);
      await page.evaluate(() => document.querySelector('.ce-root [data-act="cancel"]').click());
      await page.waitForFunction(() => !document.querySelector('.ce-root'), null, { timeout: 20000 }).catch(() => {});
      await R.settle(page, 500);
      // (item points are y down: back item -> charm -> front item; a left-right mirror of the picture is a left-right mirror of the items)
      return { BF: mul(inv(r.front), r.back), front: r.front, back: r.back };
    };
    /** The work area's Front (the switch over the grid), drawn: its canvas, and the face it says it shows. */
    const workFront = async name => {
      await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 600);
      // (ENGVIEW: the grid's switch is gone; the face is turned by Engrave.showFace, as the card's thumbnails do)
      const has = await page.evaluate(Q => { const c = document.querySelector(Q), j = c && Engrave.items().get(c.dataset.key); return !!(j && typeof Engrave.showFace === 'function' && Engrave.showFace(j, 'front', { animate: false }).ok); }, Q);
      if (has) await R.settle(page, 500);
      const o = await page.evaluate(Q => { const cv = document.querySelector(Q + ' .backHost canvas'); return cv ? { face: cv._viewFace || null, w: cv.width, h: cv.height } : null; }, Q);
      await R.shot(w, name);
      if (has) { await page.evaluate(Q => { const c = document.querySelector(Q), j = c && Engrave.items().get(c.dataset.key); if (j) Engrave.showFace(j, 'back', { animate: false }); }, Q); await R.settle(page, 400); }
      return { has: !!has, o };
    };

    /* U3 */
    const e0 = await editor('UP-3-editor-system');
    check(`U3 the Charm Editor's Front face is its Back face mirrored left-right: ${fmt(e0.BF)}`, hMirror(e0.BF), JSON.stringify(e0));
    const wf0 = await workFront('UP-3-work-front-system');
    check(`U3 the work area's Front (the switch) draws the front (${JSON.stringify(wf0.o)})`, wf0.has && wf0.o && wf0.o.face === 'front', JSON.stringify(wf0));

    /* U4 · Paul's case: the back turned right side up */
    const t = await page.evaluate(pid => { const j = Engrave.jobsOfPiece(pid).back, o = Engrave.turnTo(j, 180); Engrave.repaintTurn(j); return o; }, pid);
    await R.settle(page, 600);
    const f1 = await faces(), r1 = relate(f1);
    check(`U4 the back turned 180° (${JSON.stringify(t)}; up ${f1.back.up.toFixed(3)}°): the Front box follows it, mirrored left-right: ${fmt(r1.BF)} (${r1.miss.toFixed(4)} pt)`, t.ok && hMirror(r1.BF) && r1.miss < 0.02, JSON.stringify({ t, BF: r1.BF, miss: r1.miss }));
    // (UPRIGHT2, Paul 29 Oct 2026: the system now stands the bird right side up by itself (face-upright-default.cjs), so the person's 180° turn here turns both faces 180° from it, together)
    const w180 = d => Math.abs((((d % 360) + 540) % 360) - 180);
    check(`U4 ... and both faces turn together, 180° from the system's (back turned ${r1.backTurn.toFixed(2)}°, front ${r1.frontTurn.toFixed(2)}°; was ${r0.backTurn.toFixed(2)}° / ${r0.frontTurn.toFixed(2)}°)`,
      w180(r1.backTurn - r0.backTurn - 180) < 1e-6 && w180(r1.frontTurn - r0.frontTurn - 180) < 1e-6 && Math.abs(r1.backTurn - r1.frontTurn) < 1e-6, JSON.stringify(r1));
    await R.shot(w, 'UP-4-turned-card');
    const e1 = await editor('UP-4-editor-turned');
    check(`U4 the Charm Editor's Front face follows the turned back, mirrored left-right: ${fmt(e1.BF)}`, hMirror(e1.BF), JSON.stringify(e1));
    const wf1 = await workFront('UP-4-work-front-turned');
    check(`U4 the work area's Front draws (${JSON.stringify(wf1.o)})`, wf1.has && wf1.o && wf1.o.face === 'front', JSON.stringify(wf1));

    /* U5 · the laser */
    const fpNow = (await faces()).fp;
    check('U5 every picture above left the job\'s view, mask and fit as the laser reads them untouched', fpNow === f1.fp, '');
    await R.openEngraveDetail(w, 'sil', { face: 'back' }); await R.settle(page, 600);
    await page.click(Q + ' [data-a="approve"]', { timeout: 5000 });
    const t0 = Date.now(); let ai = null;
    while (Date.now() - t0 < 90000) { ai = [...st.blobs.keys()].find(k => /\/back\/.*\.ai$/.test(k)); if (ai) break; await new Promise(r => setTimeout(r, 300)); }
    let sha = '';
    if (ai) {
      const at = await page.evaluate(pid => String(Engrave.jobsOfPiece(pid).back.approvedAt || ''), pid);
      // (the file's own dates and the approval's time are the run's, not the engraving's: taken out before the bytes are compared)
      const txt = st.blobs.get(ai).buf.toString('latin1').replace(/\/(CreationDate|ModDate) \([^)]*\)/g, '/$1 ()').split(at).join('AT').split([...at].map(ch => '00' + ch.charCodeAt(0).toString(16).toUpperCase()).join('')).join('AT').replace(/<xmp:(CreateDate|ModifyDate|MetadataDate)>[^<]*</g, '<xmp:$1><');
      sha = crypto.createHash('sha256').update(txt, 'latin1').digest('hex');
      if (process.env.LASER_OUT) require('fs').writeFileSync(process.env.LASER_OUT, txt, 'latin1');
    }
    console.log(`LASER back file ${ai || '(none)'} sha256 (dates out) ${sha}`);
    check(`U5 approved: the back file is written (${ai ? ai.split('/').pop() : 'none'})`, !!ai, '');
    check('errors on the page: none', !w.errors.length, JSON.stringify(w.errors).slice(0, 400));
  } finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(bad.length ? `FAILED ${bad.length} of ${RESULTS.length}: ${bad.map(r => r.id.slice(0, 40)).join(' | ')}` : `PASS ${RESULTS.length} checks: UPRIGHT — the front and the back of a piece are each other's left-right mirror in every picture, the sheet's turn never reaches them, the laser unchanged`);
  if (bad.length) process.exitCode = 1;
})().catch(e => { console.error(e); process.exit(1); });
