// MONOGRAM piece-size policy (MONOX E4, Paul, 16 Oct 2026): opts.fit = { maxWidthMm, maxHeightMm } scales the FINISHED piece (one scale for both axes), the layout the engine judged stays as it was.
// Pure geometry, offline, no browser, synthetic block letters:
//   · fitScale / fitFromSize / cleanFit: the number alone, the SIZEFIT token as an envelope, nonsense ignored,
//   · build with opts.fit: size, proportions, rings / placed / contact marks scaled, report.fit, metrics untouched; a roomy envelope changes nothing; height can be the limit; grow fills the envelope,
//   · autoFit with opts.fit, toPdf of a fitted piece (page size), and the engine without the module (a warning, the piece unscaled).
//   node tests/charm-nest/monogram-fit.cjs
'use strict';
const assert = require('assert');
const M = require('../../charm-nest-monogram.js');
const F = require('../../charm-nest-monogram-fit.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const near = (a, b, tol, m) => { assert(Math.abs(a - b) <= tol, `${m}: ${a} vs ${b}`); n++; };
const MM = 25.4 / 72;

// ── the page loads the module after the engine, and the public build ships it ──────────────────────────────────────────────────
{
  const fs = require('fs'), path = require('path'), root = path.resolve(__dirname, '../..');
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), build = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');
  const at = re => { const m = re.exec(html); return m ? m.index : -1; };
  ok(at(/<script src="charm-nest-monogram\.js\?v=[^"]*-mx4"><\/script>/) >= 0 && at(/<script src="charm-nest-monogram-fit\.js\?v=[^"]*-mx4"><\/script>/) > at(/<script src="charm-nest-monogram\.js/) && at(/<script src="charm-nest-monogram-ui\.js/) > at(/<script src="charm-nest-monogram-fit\.js/), 'charm-nest-1.html: the fit module is loaded after the engine and before the card (cache token -mx4)');
  ok(/"charm-nest-monogram-fit\.js"/.test(build), 'scripts/build-public.cjs ships the fit module');
}

// ── the number alone ──────────────────────────────────────────────────────────────────────────────────────────────────────────
ok(JSON.stringify(F.ENVELOPES) === '[16,18,20]', 'the three envelopes offered');
let s = F.fitScale(22, 14.8, { maxWidthMm: 16 }); near(s.scale, 16 / 22, 1e-9, 'width limits'); ok(s.limiting === 'width', 'limiting: width');
s = F.fitScale(22, 14.8, { maxWidthMm: 30 }); ok(s.scale === 1 && s.limiting === 'none', 'a roomy envelope: no change');
s = F.fitScale(22, 14.8, { maxWidthMm: 30, grow: true }); near(s.scale, 30 / 22, 1e-9, 'grow fills the envelope');
s = F.fitScale(16, 20, { maxWidthMm: 18, maxHeightMm: 15 }); near(s.scale, 0.75, 1e-9, 'height limits'); ok(s.limiting === 'height', 'limiting: height');
s = F.fitScale(22, 14.8, { maxWidthMm: 20, maxHeightMm: 12 }); near(s.scale, 12 / 14.8, 1e-9, 'the smaller of the two scales'); ok(s.limiting === 'height', 'the tighter side limits');
s = F.fitScale(22, 14.8, { maxWidthMm: 16, maxHeightMm: 12 }); near(s.scale, 16 / 22, 1e-9, 'the smaller of the two scales (width)'); ok(s.limiting === 'width', 'the tighter side limits (width)');
for (const bad of [null, undefined, 5, 'x', {}, { maxWidthMm: 0 }, { maxWidthMm: -3 }, { maxWidthMm: NaN }, { maxWidthMm: '18' }, { maxHeightMm: Infinity }, { maxWidthMm: 0.1 }]) { ok(F.cleanFit(bad) === null, 'nonsense is no envelope: ' + JSON.stringify(bad)); ok(F.fitScale(22, 14.8, bad).scale === 1, 'and scales nothing'); }
ok(JSON.stringify(F.fitFromSize('18MM')) === '{"maxWidthMm":18}' && F.fitFromSize('1.8CM').maxWidthMm === 18 && F.fitFromSize('12.5mm').maxWidthMm === 12.5, 'SIZEFIT tokens are envelopes');
for (const bad of ['', null, 'LARGE', '18', '18 MM EXTRA', '3MM', '99MM', '0MM']) ok(F.fitFromSize(bad) === null, 'not a size: ' + bad);
ok(F.fitFromSize('20MM', { maxHeightMm: 16 }).maxHeightMm === 16, 'extra keys ride along');

// ── a real build, synthetic letters ───────────────────────────────────────────────────────────────────────────────────────────
const rect = (x0, y0, x1, y1) => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]];
const A = { letter: 'A', rings: [[[0, 0], [5, 0], [6.5, 9], [13.5, 9], [15, 0], [20, 0], [11.5, 30], [8.5, 30]], [[8.2, 14], [11.8, 14], [10, 23]]] };
const Pp = { letter: 'P', rings: [[[0, 0], [5, 0], [5, 12], [12, 12], [17, 16], [17, 26], [12, 30], [0, 30]], [[5, 17], [11, 17], [11, 25], [5, 25]]] };
const V = { letter: 'V', rings: [[[0, 30], [5, 30], [10, 10], [15, 30], [20, 30], [12, 0], [8, 0]]] };
const trio = { left: A, middle: Pp, right: V };
const OPT = { centerRatio: 1.5, sideWiden: 1, contacts: 1, allowTrim: false, normalise: false };
const base = M.build(trio, OPT);
ok(base.ok && !base.report.fit, 'the plain build has no report.fit');
const k = 0.6, want = base.wMm * k;
const r = M.build(trio, Object.assign({ fit: { maxWidthMm: want } }, OPT));
ok(r.ok, 'a fitted build is built');
near(r.wMm, want, 1e-3, 'width is the envelope'); near(r.hMm, base.hMm * k, 1e-3, 'height keeps the proportion'); near(r.wMm / r.hMm, base.wMm / base.hMm, 1e-3, 'aspect unchanged');
ok(r.report.fit && r.report.fit.applied && Math.abs(r.report.fit.scale - k) < 1e-3 && r.report.fit.limiting === 'width', 'report.fit: applied, the scale, the limiting side');
ok(r.report.fit.designMm[0] === Math.round(base.wMm * 1000) / 1000 && r.report.fit.sizeMm[0] === Math.round(r.wMm * 1000) / 1000, 'report.fit: design size and final size');
ok(r.rings.length === base.rings.length && r.rings.every((g, i) => g.hole === base.rings[i].hole && g.pts.length === base.rings[i].pts.length), 'same rings, same points');
ok(r.rings.every((g, i) => g.pts.every((p, j) => Math.abs(p[0] - base.rings[i].pts[j][0] * k) < 2e-4 && Math.abs(p[1] - base.rings[i].pts[j][1] * k) < 2e-4)), 'every point scaled by the one factor');
ok(r.bboxPt[0] === 0 && r.bboxPt[1] === 0 && Math.abs(r.wMm - r.bboxPt[2] * MM) < 1e-3 && Math.abs(r.hMm - r.bboxPt[3] * MM) < 1e-3, 'bbox still starts at 0,0 and is wMm x hMm');
ok(r.placed.every((p, i) => p.bboxPt.every((v, j) => Math.abs(v - base.placed[i].bboxPt[j] * k) < 2e-4) && Math.abs(p.scale - base.placed[i].scale * k) < 2e-4), 'placed letters scaled (box and letter scale)');
ok(base.report.metrics === null && r.report.metrics === null, 'the classic geometry has no metrics (the metric checks are on the normalised build below)');
ok(r.report.warnings.length >= base.report.warnings.length, 'warnings only grow');
ok(r.report.notes.some(t => /^scaled to 60 % to fit .* mm wide \(drawn /.test(t)) && !base.report.notes.some(t => /^scaled to/.test(t)), 'a note says what was done');
// the normalised build (today's defaults, with the glyph metrics): the judged layout stays, the marks follow the piece
const withM = (l, b) => Object.assign({ metrics: { baselinePt: b, emPt: 53 } }, l);
const ntrio = { left: withM(A, 0), middle: withM(Pp, 0), right: withM(V, 0) };
const nb = M.build(ntrio, {}), nr = M.build(ntrio, { fit: { maxWidthMm: nb.wMm * 0.7 } });
ok(nb.ok && nr.ok && nb.report.metrics && nr.report.metrics, 'normalised builds have metrics');
near(nr.wMm, nb.wMm * 0.7, 1e-3, 'normalised: width is the envelope');
ok(JSON.stringify(nr.report.metrics.violations) === JSON.stringify(nb.report.metrics.violations) && nr.report.metrics.piece.aspect === nb.report.metrics.piece.aspect && nr.report.metrics.severity === nb.report.metrics.severity && nr.report.metrics.piece.wMm === nb.report.metrics.piece.wMm, 'the judged layout (metrics) is untouched');
const c0 = nb.report.metrics.contacts, c1 = nr.report.metrics.contacts;
ok(c0.length > 0 && c1.length === c0.length && c1.every((c, i) => Math.abs(c.xPt - c0[i].xPt * 0.7) < 2e-3 && Math.abs(c.yPt - c0[i].yPt * 0.7) < 2e-3 && c.zone === c0[i].zone), 'contact marks follow the piece');
ok(c1.every(c => c.xPt >= -1e-6 && c.xPt <= nr.bboxPt[2] + 1e-6 && c.yPt >= -1e-6 && c.yPt <= nr.bboxPt[3] + 1e-6), 'and stay inside it');
const j0 = nb.report.joinMm, j1 = nr.report.joinMm;
ok(j0 && Object.keys(j0).every(key => j0[key] == null || Math.abs(j1[key] - j0[key] * 0.7) < 2e-3), 'join widths follow the piece (mm)');
ok(nr.report.fit.strokeMm.design > 0 && Math.abs(nr.report.fit.strokeMm.final - nr.report.fit.strokeMm.design * 0.7) < 2e-3, 'the thinnest letter\'s mean line is reported at both sizes');
// idempotent and deterministic
const r2 = M.build(trio, Object.assign({ fit: { maxWidthMm: want } }, OPT));
ok(JSON.stringify(r2.rings) === JSON.stringify(r.rings) && r2.wMm === r.wMm, 'same input, same fitted piece');
const again = F.applyFit(JSON.parse(JSON.stringify(r)), { maxWidthMm: want });
ok(Math.abs(again.wMm - want) < 1e-3 && again.report.fit.applied === false, 'fitting a fitted piece again changes nothing');

// roomy, height, grow
const roomy = M.build(trio, Object.assign({ fit: { maxWidthMm: 100 } }, OPT));
ok(JSON.stringify(roomy.rings) === JSON.stringify(base.rings) && roomy.wMm === base.wMm && roomy.report.fit.applied === false && roomy.report.fit.scale === 1, 'an envelope bigger than the piece changes nothing (report says so)');
const tall = M.build(trio, Object.assign({ fit: { maxWidthMm: 100, maxHeightMm: base.hMm / 2 } }, OPT));
near(tall.hMm, base.hMm / 2, 1e-3, 'height can be the limit'); ok(tall.report.fit.limiting === 'height', 'report.fit says height');
const big = M.build(trio, Object.assign({ fit: { maxWidthMm: base.wMm * 1.5, grow: true } }, OPT));
near(big.wMm, base.wMm * 1.5, 1e-3, 'grow fills the envelope');
const junk = M.build(trio, Object.assign({ fit: { maxWidthMm: 'big', maxHeightMm: -2 } }, OPT));
ok(junk.ok && junk.wMm === base.wMm && !junk.report.fit, 'a nonsense envelope is ignored, the piece is the plain one');
// a small scale warns, with the numbers
const tiny = M.build(trio, Object.assign({ fit: { maxWidthMm: base.wMm * 0.4 } }, OPT));
ok(tiny.report.fit.warnings.some(w => /scaled to 40 %/.test(w)) && tiny.report.warnings.some(w => /scaled to 40 %/.test(w)), 'a strong shrink is said in the warnings');
ok(!r.report.fit.warnings.some(w => /scaled to/.test(w)) || k < F.POLICY.WARN_SCALE, 'a mild shrink is not');

// autoFit passes the envelope through
const af = M.autoFit(ntrio, { fit: { maxWidthMm: 12 }, autoFitMs: 800 });
ok(af.ok && af.wMm <= 12 + 1e-3 && af.report.fit && af.report.fit.maxWidthMm === 12, 'autoFit result fits the envelope');

// the file: the page is the piece plus the page rule's margin
(async () => {
  const bytes = await M.toPdf(r, { title: 'fit' }), { PDFDocument } = require('../../vendor/pdf-lib-1.17.1.min.js');
  const doc = await PDFDocument.load(bytes), sz = doc.getPage(0).getSize(), bw = r.bboxPt[2], bh = r.bboxPt[3], pad = Math.max(0.5 / 2 + 2, 0.125 * Math.max(bw, bh));
  near(sz.width, bw + 2 * pad, 0.01, 'PDF page width = piece + margins'); near(sz.height, bh + 2 * pad, 0.01, 'PDF page height');
  // the engine without the module: the piece is plain, and says so
  const Module = require('module'), path = require('path'), file = path.resolve(__dirname, '../../charm-nest-monogram.js'), fitFile = path.resolve(__dirname, '../../charm-nest-monogram-fit.js');
  delete require.cache[file]; delete require.cache[fitFile]; delete globalThis.CharmNestMonogramFit;
  const orig = Module._resolveFilename; Module._resolveFilename = function (req, ...a) { if (/charm-nest-monogram-fit/.test(req)) throw new Error('not here'); return orig.call(this, req, ...a); };
  let lr; try { lr = require(file).build(trio, Object.assign({ fit: { maxWidthMm: 10 } }, OPT)); } finally { Module._resolveFilename = orig; }
  ok(lr.ok && lr.wMm === base.wMm && lr.report.warnings.some(w => /opts\.fit was not applied/.test(w)), 'without the module the piece stays plain and the warning says why');
  console.log(`monogram-fit: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
