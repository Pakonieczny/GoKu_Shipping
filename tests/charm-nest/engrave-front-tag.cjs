// ENGFRONT (13 Oct 2026, Paul's second report): "The Front engraving is not showing in the work area, the back engraving works fine".
//
// The order: the Mini Tag listing, Charm Type "Tag1 (front+back)", personalisation front "W" / back a date, tag 5X8.5. Pressing the Front thumbnail: the card's thumbnail drew the "W"; the work area
// drew the empty charm (outline and hole, no words, no tint, no box), Save and the position buttons greyed.
//
// Why: the real master "5X8.5" has an ENGRAVE layer that copies the cut outline and the hole (closed lines, no fill). The FRONT keeps its words off the design's own engraved art and treated every closed
// shape as filled: the copy of the outline took the whole body, the front's usable area was empty, the fit failed ("The text could not fit inside the usable back area.") and the job stood in state review with
// no fit, so the work area had nothing to draw. (The thumbnail does not use the usable area, so it still drew the "W".) The back never subtracts the art, so the back was fine.
//
// Fix (charm-nest-geom.js engraveMask): a closed line with no fill drawn round (almost) the whole body is a frame, not a figure: only its own line is kept clear. Smaller closed shapes and every filled shape
// keep their whole inside clear, as before.
//
//   A  pure geometry (no browser): the tag's ENGRAVE copy of its outline leaves the front usable; an inset frame keeps its own line clear; a small ring and a big filled shape still keep their inside clear;
//      the back's mask is unchanged
//   B  the real page over the fake site with the REAL 5X8.5 master (tests/charm-nest/fixtures/MASTER_5X8_5.ai, the sorter's own master file): the Front job is fitted, the Front thumbnail draws the front with its
//      words, box and Approve, the Back still draws, "Approve both faces" gives one seal per face. Offline only, no paid call (counted), no Etsy; made-up names, "W" is a plain test string.
//
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-front-tag.cjs      (A alone: FT_ONLY=A; SHOTS=<dir> for screenshots)
//   Exit code 1 while any check is red; every line is PASS|FAIL id evidence.
'use strict';
const path = require('path'), fs = require('fs');
const RESULTS = [];
const realLog = console.log;
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); realLog(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${evidence}`); };
const ONLY = process.env.FT_ONLY || '';

/* ═══ A · geometry ═══ */
function partA() {
  const G = require(path.join(__dirname, '../../charm-nest-geom.js'));
  const rect = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['l', [x0, y0]]];
  const circ = (cx, cy, r) => { const k = 0.5523 * r; return [['m', [cx + r, cy]], ['c', [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ['c', [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ['c', [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ['c', [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]]]; };
  const bbOf = sub => { let x0 = 1e9, y0 = 1e9, x1 = -1e9, y1 = -1e9; for (const sg of sub) for (let i = 1; i < sg.length; i++) { const p = sg[i]; x0 = Math.min(x0, p[0]); y0 = Math.min(y0, p[1]); x1 = Math.max(x1, p[0]); y1 = Math.max(y1, p[1]); } return [x0, y0, x1, y1]; };
  const mem = (sub, extra) => Object.assign({ kind: 'path', layer: 'CUT', stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.25, paintOp: 'S', closed: true, subpaths: [sub], bbox: bbOf(sub) }, extra || {});
  const line = sub => mem(sub, { layer: 'ENGRAVE', strokeRGB: [1, 0, 0] });                                  // (engraved art drawn as a line: ENGRAVE layer, red stroke, no fill)
  const solid = sub => mem(sub, { layer: 'ENGRAVE', stroke: false, fill: true, fillRGB: [0, 0, 1] });         // (engraved art drawn filled: hatching)
  const charm = (body, holes, art) => { const o = mem(body), hs = holes.map(h => mem(h)); return { outline: o, members: [o, ...hs, ...(art || [])], bbox: o.bbox, widthPt: o.bbox[2] - o.bbox[0], heightPt: o.bbox[3] - o.bbox[1] }; };
  const mask = (c, face) => { const v = G.backView(c, face ? { res: 6, face } : { res: 6 }); return { v, m: G.engraveMask(v, { marginMm: .8 }) }; };

  // the real mini tag (5X8.5): 16.9 x 26.6 pt, the hanging hole near the top end; the ENGRAVE layer copies the outline (0.22 pt out) and the hole
  const tag = (art) => charm(rect(3, 3, 19.9, 29.6), [circ(11.45, 24.5, 1.7)], art);
  const copy = [line(rect(3.22, 3.22, 19.68, 29.38)), line(circ(11.45, 24.5, 1.9))];
  const plain = mask(tag([]), 'front'), framed = mask(tag(copy), 'front');
  const aP = G.area(plain.m), aF = G.area(framed.m);
  check('A1 a mini tag whose ENGRAVE layer copies its outline and hole: the front keeps its usable area (it was empty)', aP > 0 && aF >= 0.75 * aP, `usable pixels plain ${aP}, with the copy ${aF} (the margin now counts from the line as well: a little less, never nothing)`);
  check('A2 the same tag\'s BACK is exactly what it was (the back never subtracts the art)', G.area(mask(tag(copy)).m) === G.area(mask(tag([])).m) && G.area(mask(tag(copy)).m) > 0, `${G.area(mask(tag(copy)).m)} vs ${G.area(mask(tag([])).m)}`);

  // an inset frame (a line 6 pt inside the cut line of a big charm, 88 % of the body): the words may go inside it, never over its own line
  const big = (art) => charm(rect(0, 0, 200, 200), [], art);
  const fr = mask(big([line(rect(6, 6, 194, 194))]), 'front'), bare = mask(big([]), 'front');
  const at = (c, x, y) => G.at(c.m, x, y);   // (a square with no hole is not turned, so the drawn coordinates are the mask's)
  check('A3 an inset frame line: the inside of the frame is usable, its own line is kept clear (the same spot is usable without the frame)', at(fr, 100, 100) === 1 && at(fr, 30, 100) === 1 && at(fr, 6, 100) === 0 && at(fr, 100, 6) === 0 && at(bare, 6, 100) === 1,
    `centre ${at(fr, 100, 100)}, inside ${at(fr, 30, 100)}, on the line ${at(fr, 6, 100)}, without the frame ${at(bare, 6, 100)}`);
  check('A4 …and it costs only a strip along its line (the usable pixels stay at least 85% of the same charm without the frame)', G.area(fr.m) >= 0.85 * G.area(bare.m) && G.area(fr.m) < G.area(bare.m), `${G.area(fr.m)} vs ${G.area(bare.m)}`);

  // a small closed line (a ring round a fifth of the body) and a big filled shape keep their whole inside clear, as before
  const ring = mask(big([line(circ(100, 100, 45))]), 'front');
  check('A5 a small closed line (a ring, 16 % of the body) still keeps its whole inside clear', G.at(ring.m, 100, 100) === 0 && G.at(ring.m, 100, 60) === 0 && G.at(ring.m, 30, 30) === 1, `centre ${G.at(ring.m, 100, 100)}, near the ring ${G.at(ring.m, 100, 60)}, away ${G.at(ring.m, 30, 30)}`);
  const slab = mask(big([solid(rect(6, 6, 194, 194))]), 'front');
  check('A6 a FILLED shape over 88 % of the body still keeps all of it clear (only a line is a frame)', G.at(slab.m, 100, 100) === 0 && G.at(slab.m, 30, 100) === 0, `centre ${G.at(slab.m, 100, 100)}, inside ${G.at(slab.m, 30, 100)}`);
}

/* ═══ B · the real page, the real 5X8.5 ═══ */
async function partB() {
  const R = require('./_engraveRealWorld.cjs');
  // the Mini Tag's master "5X8.5" is the sorter's own file (index entry as the index wrote it), not the stand-in the shared fixtures use
  R.DESIGNS['5X8.5'] = { file: 'MASTER_5X8_5.ai', make: () => fs.readFileSync(path.join(__dirname, 'fixtures/MASTER_5X8_5.ai')),
    entry: { sku: '5X8.5', widthPt: 16.8939, heightPt: 26.6179, areaPt2: 356.3888888888889, members: 4, holes: 1, engravable: true, engravableBy: 'index', upAngle: 90.00068183078804, upSource: 'hole', charmHash: '061cfa08', backKeepOut: [] } };
  let modelCalls = 0, counting = false;
  console.log = (...a) => { if (counting && /\[charmEngrave\].*(engraveIntent|engraveReview|labelRead|packing|grouping|layout)/.test(String(a[0]))) modelCalls++; return realLog.apply(console, a); };
  const Q = '#egQueue .rvItem[data-kind=placement]';
  const workFacts = page => page.evaluate(Q => {
    const vis = e => { if (!e) return false; const b = e.getBoundingClientRect(), st = getComputedStyle(e); return b.width > 0 && b.height > 0 && st.visibility !== 'hidden' && st.display !== 'none' && !e.closest('[hidden]'); };
    const c = document.querySelector(Q); if (!c) return { open: false };
    const host = c.querySelector('.backHost'), cv = host && host.querySelector('canvas'), ap = c.querySelector('[data-a="approve"]');
    return { open: true, job: c.dataset.key, state: c.dataset.state, canvas: !!(cv && vis(cv)), box: !!(cv && cv._box), text: host ? host.textContent.replace(/\s+/g, ' ').trim().slice(0, 160) : '', approve: !!(ap && vis(ap)),
      current: ((([...c.querySelectorAll('.engCard .engFace')].find(f => f.hasAttribute('aria-current'))) || {}).dataset || {}).face || null, why: [...c.querySelectorAll('.ctl .why')].map(x => x.textContent.trim()).join(' | '), centre: null };
  }, Q);
  const jobsOf = (page, key) => page.evaluate(key => Engrave.jobsOf(B.orders.byKey.get(key)).map(j => ({ key: j.key, face: j.face === 'front' ? 'front' : 'back', state: j.state, text: j.text, reason: j.reason || null,
    glyphs: j.fit && j.fit.glyphs ? j.fit.glyphs.length : 0, fitted: !!(j.fit && j.fit.glyphs && j.fit.glyphs.length), centre: j.fit && j.fit.centre ? j.fit.centre.map(v => Math.round(v * 1000) / 1000) : null,
    onMask: j.fit && j.mask && j.fit.glyphs ? j.fit.glyphs.every(g => g.cmds.every(c => c.type === 'Z' || CharmNestGeom.at(j.mask, c.x, c.y) === 1)) : null, seals: (j.engravingSeals || []).filter(s => s.how === 'engraveApproved').length, by: j.approvedBy || null })), key);
  const press = (page, face) => page.evaluate(({ Q, f }) => { const b = document.querySelector(Q + ' .engCard .engFace[data-face="' + f + '"] .engThumb'); if (b) b.click(); return !!b; }, { Q, f: face });
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 300)); } };

  const w = await R.world({ shots: process.env.SHOTS || '/tmp/engfront-shots' });
  if (!w) { realLog('SKIP no playwright-core'); return; }
  const page = w.page;
  try {
    const made = await R.build(w, ['tag']);
    const key = made.tag.key; counting = true;
    const j0 = await jobsOf(page, key);
    const f0 = j0.find(j => j.face === 'front'), b0 = j0.find(j => j.face === 'back');
    check('B1 the real 5X8.5 tag (front "W", back a date): both faces have a FIT (the front had none: "could not fit inside the usable back area")', !!f0 && !!b0 && f0.fitted && b0.fitted && !f0.reason && f0.text === 'W', JSON.stringify(j0.map(j => [j.face, j.state, j.glyphs, j.reason])));
    check('B2 the front\'s words lie on the front\'s usable area (every point of every letter on the mask)', f0 && f0.onMask === true && b0 && b0.onMask === true, JSON.stringify([f0 && f0.onMask, b0 && b0.onMask]));
    await R.openEngraveDetail(w, 'tag', { face: 'front' }); await R.settle(page, 900);
    const wf = await workFacts(page);
    await R.shot(w, 'TAG-front-pressed');
    check('B3 the Front thumbnail pressed: the work area is the FRONT job, drawn, with the words\' box and the Approve button, and no "could not fit" line', wf.current === 'front' && /~front$/.test(wf.job) && wf.canvas && wf.box && wf.approve && !wf.text && !wf.why, JSON.stringify(wf));
    await press(page, 'back'); await R.settle(page, 600);
    const wb = await workFacts(page);
    check('B4 the Back thumbnail still draws the back (its own job, box, Approve)', wb.current === 'back' && !/~front$/.test(wb.job) && wb.canvas && wb.box && wb.approve, JSON.stringify(wb));
    await press(page, 'front'); await R.settle(page, 600);
    const wf2 = await workFacts(page), j1 = await jobsOf(page, key);
    check('B5 front, back, front: the front is drawn again, its place unchanged', wf2.canvas && wf2.box && JSON.stringify(j1.find(j => j.face === 'front').centre) === JSON.stringify(f0.centre), JSON.stringify({ wf2: [wf2.current, wf2.canvas, wf2.box], centre: [f0.centre, j1.find(j => j.face === 'front').centre] }));
    // Approve both faces: one seal for each, permanent
    const both = await page.evaluate(Q => { const b = document.querySelector(Q + ' [data-a="approveBoth"], ' + Q + ' [data-a="approveFaces"]'); if (b) b.click(); return !!b; }, Q);
    await until(async () => (await jobsOf(page, key)).every(j => ['approved', 'written'].includes(j.state)), 90000);
    await R.settle(page, 800);
    const j2 = await jobsOf(page, key);
    check('B6 "Approve both faces": the front and the back approved, exactly ONE seal each, by the signed-in name', both && j2.length === 2 && j2.every(j => ['approved', 'written'].includes(j.state) && j.seals === 1 && j.by), JSON.stringify({ both, j2: j2.map(j => [j.face, j.state, j.seals, j.by]) }));
    check('B7 no model call was made by any of this (the front takes its words from the setup)', modelCalls === 0, 'calls after the world was built: ' + modelCalls);
    check('B8 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 300));
  } finally { console.log = realLog; await w.close(); }
}

(async () => {
  if (ONLY !== 'B') partA();
  if (ONLY !== 'A') await partB();
  const bad = RESULTS.filter(r => !r.ok);
  realLog(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.log = realLog; console.error(e); process.exit(1); });
