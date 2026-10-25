// Paul's master "SKUs_BEADY_2026-1023.ai" (25 Oct 2026: 418 charm outlines, 1,391 SKU lines), dropped in the Master tab, came back with these errors, and these are what this test holds fixed
// (the sheet itself is not a fixture: every case is drawn here, at the size and in the place the master had it):
//   · a label set just under the curve of a round ornament (Joy_53599) was "no charm above": a path's box is taken from its Bézier control points, which reach 3.4 pt below the circle,
//     so the label stood 3.3 pt "inside" the box. Measured against the outline's own lower edge it is under it (5 pt into a flat edge is still not: label-slash-overlap.cjs);
//   · a label set between a horse's legs (Jumping_35230) was no label of it, and became the "title" of the charm below;
//   · a yin-yang drawn as two outlines meeting along an S with ONE ring (PHOENIX_30705): the label went to the half without the ring, so the library got half a phoenix with no hanging
//     hole. The halves are one charm and one cut line. Two halves that each carry a ring are two pieces (a broken heart) and stay as they are;
//   · words written ON a design with its own SKU ("The Martins" on a cottage, the days of a calendar) were 43 "orphan labels": they are its live text, said as such (onDesign);
//   · a design drawn twice with one label under both: the second drawing's "no SKU" now says why;
//   · STANDARD SIZE (Paul: "Most charms should be approximately 10.5-11.5mm unless they are very long and narrow"): a body longer than 11.5 mm is drawn at 11.5 mm when it is indexed, its
//     ring (or a hanging hole punched in it) at its own size; a smaller body, a long and narrow one, one whose label names a size, and a body touching one without a SKU keep the size drawn.
// Offline: synthetic pages built here, nothing is sent anywhere.
//   node tests/charm-nest/master-beady-1023.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const { scene } = require('./_masterScene.cjs');
const { CharmNestPDF: P, Geom: G } = require('../../netlify/functions/_charmNestPdf.js');
const V = require('../../charm-nest-vector.js'), Pair = require('../../charm-nest-pair.js');
const MM = 25.4 / 72, PT = 72 / 25.4;
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const f = v => (+v).toFixed(3), pen = d => `q 0.05 0.05 0.05 RG 0.5 w ${d} S Q`;
const polyOps = pts => pts.map((p, i) => `${f(p[0])} ${f(p[1])} ${i ? 'l' : 'm'}`).join(' ') + ' h';
const arc = (cx, cy, r, a0, a1, steps) => Array.from({ length: steps + 1 }, (_, i) => { const a = (a0 + (a1 - a0) * i / steps) * Math.PI / 180; return [cx + r * Math.cos(a), cy + r * Math.sin(a)]; });
const circleOps = (cx, cy, r) => { const k = 0.5523 * r; return `${f(cx + r)} ${f(cy)} m ${f(cx + r)} ${f(cy + k)} ${f(cx + k)} ${f(cy + r)} ${f(cx)} ${f(cy + r)} c ${f(cx - k)} ${f(cy + r)} ${f(cx - r)} ${f(cy + k)} ${f(cx - r)} ${f(cy)} c ${f(cx - r)} ${f(cy - k)} ${f(cx - k)} ${f(cy - r)} ${f(cx)} ${f(cy - r)} c ${f(cx + k)} ${f(cy - r)} ${f(cx + r)} ${f(cy - k)} ${f(cx + r)} ${f(cy)} c h`; };
// a circle written as THREE 120° arcs (as Illustrator writes a rotated circle): its control points reach 0.167 r past the curve
const circle3 = (cx, cy, r) => { const k = 4 / 3 * Math.tan(Math.PI / 6), at = a => [cx + r * Math.cos(a), cy + r * Math.sin(a)]; let s = ''; const A = [-150, -30, 90, 210].map(d => d * Math.PI / 180);
  for (let i = 0; i < 3; i++) { const a = A[i], b = A[i + 1], p0 = at(a), p3 = at(b), p1 = [p0[0] - k * r * Math.sin(a), p0[1] + k * r * Math.cos(a)], p2 = [p3[0] + k * r * Math.sin(b), p3[1] - k * r * Math.cos(b)]; if (!i) s += `${f(p0[0])} ${f(p0[1])} m `; s += `${f(p1[0])} ${f(p1[1])} ${f(p2[0])} ${f(p2[1])} ${f(p3[0])} ${f(p3[1])} c `; }
  return s + 'h'; };
// a yin-yang of two closed outlines meeting along an S (radius r): [left half, right half]
const yinYang = (cx, cy, r) => { const S = arc(cx, cy - r / 2, r / 2, -90, 90, 24).concat(arc(cx, cy + r / 2, r / 2, 270, 90, 24).slice(1));   // bottom → top: the lower bulge right, the upper left
  return [arc(cx, cy, r, 90, 270, 48).concat(S.slice(1, -1)), arc(cx, cy, r, 90, -90, 48).concat(S.slice(1, -1))]; };
const read = async (bytes, gapPt) => { const parsed = await P.parseSource(bytes, 'beady.ai'), g = P.groupCharms(parsed, { minPt: 6 }), lab = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: gapPt || 6.4 * PT, widen: 0.25 }); return { parsed, g, lab, live: g.charms.filter(c => c.mergedInto == null), bySku: sku => g.charms.find(c => c.mergedInto == null && lab.labels.has(c.index) && [lab.labels.get(c.index)].concat(lab.labels.get(c.index).extra || []).some(x => x.sku === sku)) }; };
const trueBox = seg => G.flatten(seg, 16).flat().reduce((b, q) => [Math.min(b[0], q[0]), Math.min(b[1], q[1]), Math.max(b[2], q[0]), Math.max(b[3], q[1])], [Infinity, Infinity, -Infinity, -Infinity]);
const longMm = seg => { const b = trueBox(seg); return Math.max(b[2] - b[0], b[3] - b[1]) * MM; };

(async () => {
  // ── 1 · labels: under the curve of a round ornament, and between a horse's legs
  { const R = 24, cx = 70, cy = 150, x0 = 180, y0 = 120;
    const horse = [[x0, y0], [x0 + 4, y0], [x0 + 4, y0 + 10], [x0 + 36, y0 + 10], [x0 + 36, y0 + 3], [x0 + 40, y0 + 3], [x0 + 40, y0 + 30], [x0, y0 + 30]];
    const W = await read(await scene({ page: [300, 220], extraOps: [pen(circle3(cx, cy, R)), pen(polyOps(horse))], labels: [{ text: 'Joy_1', cx, top: cy - R - 0.3 }, { text: 'Jump_1', cx: x0 + 20, top: y0 + 8 }] }));
    const joy = W.live.find(c => Math.abs((c.outline.bbox[0] + c.outline.bbox[2]) / 2 - cx) < 3);
    ok(joy && cy - R - joy.outline.bbox[1] > 3.3, 'the ornament\'s box (its control points) ends more than 3.3 pt below the circle: ' + (joy && (cy - R - joy.outline.bbox[1]).toFixed(2)));
    ok(W.bySku('JOY_1') === joy, 'and the label just under the circle is its label');
    ok(W.bySku('JUMP_1') && W.bySku('JUMP_1').outline.bbox[0] === x0, 'the label between the legs is the horse\'s label');
    ok(W.lab.orphans.length === 0 && W.lab.titles.length === 0 && W.lab.unlabelled.length === 0, 'no line is left without a charm, none is a title, no charm without a SKU: ' + JSON.stringify({ o: W.lab.orphans.map(o => o.sku), t: W.lab.titles, u: W.lab.unlabelled })); }

  // ── 2 · the yin-yang: one ring between two halves is one charm; a ring on each half is two pieces
  { const R = 20, A = [70, 120], B = [200, 120], ring = (c, deg) => { const a = deg * Math.PI / 180, d = R + 2.75; return pen(`${circleOps(c[0] + d * Math.cos(a), c[1] + d * Math.sin(a), 3.25)} ${circleOps(c[0] + d * Math.cos(a), c[1] + d * Math.sin(a), 1.85)}`); };
    const [aL, aR] = yinYang(A[0], A[1], R), [bL, bR] = yinYang(B[0], B[1], R);
    const W = await read(await scene({ page: [300, 220], extraOps: [pen(polyOps(aL)), pen(polyOps(aR)), ring(A, 120), pen(polyOps(bL)), pen(polyOps(bR)), ring(B, 120), ring(B, 60)], labels: [{ text: 'Phoenix_1', cx: A[0] + 3, top: A[1] - R - 2 }, { text: 'Halves_1', cx: B[0] + 3, top: B[1] - R - 2 }] }));
    const ph = W.bySku('PHOENIX_1'), ab = W.lab.absorbed.find(a => /shares a seam/.test(a.why));
    ok(ph && ab && ab.into === ph.index && ab.sku === 'PHOENIX_1', 'the half with the ring joined the labelled half: ' + JSON.stringify(W.lab.absorbed));
    const ob = trueBox(ph.outline);
    ok(ph.outline.subpaths.length === 1 && ph.outline.synthetic && Math.abs(ob[2] - ob[0] - 2 * R) < 0.5 && Math.abs(ob[3] - ob[1] - 2 * R) < 0.5, 'its cut line is ONE outline round the whole disc (the seam is not cut): ' + ob.map(v => v.toFixed(1)));
    ok(!W.lab.unlabelled.some(i => Math.abs((W.g.charms[i].outline.bbox[0] + W.g.charms[i].outline.bbox[2]) / 2 - A[0]) < 15), 'and no half of it is left without a SKU');
    { const r = P.integrateRings(ph); ok(r.welded === 1 && !r.left.length, 'its ring is welded into it: ' + JSON.stringify(r)); }
    { const back = await P.parseSource(new Uint8Array(await P.buildSingleCharm(ph, W.parsed)), 'p.ai'), live = P.groupCharms(back, { minPt: 6 }).charms.filter(c => c.mergedInto == null); ok(live.length === 1, 'its design file reads back as one charm: ' + live.length); }
    const hv = W.bySku('HALVES_1');
    ok(hv && !W.lab.absorbed.some(a => a.into === hv.index), 'two halves that each carry a ring are two pieces: nothing is welded');
    ok(W.lab.unlabelled.some(i => Math.abs((W.g.charms[i].outline.bbox[0] + W.g.charms[i].outline.bbox[2]) / 2 - B[0]) < 15), 'and the other half is still a charm of its own, for a person to look at'); }

  // ── 3 · words on a design, a design drawn twice
  { const W = await read(await scene({ page: [420, 220], bodies: [{ shape: 'house', x: 30, y: 100, w: 50, h: 50 }, { shape: 'house', x: 130, y: 100, w: 50, h: 50 }, { shape: 'blob', x: 230, y: 100, w: 40, h: 40 }, { shape: 'blob', x: 330, y: 100, w: 40, h: 40 }],
      labels: [{ text: 'The Smiths', cx: 55, top: 132 }, { text: 'House_1', cx: 55, top: 98 }, { text: 'The Joneses', cx: 155, top: 132 }, { text: 'Twin_1', cx: 250, top: 98 }, { text: 'Twin_1', cx: 350, top: 98 }] }));
    ok(W.lab.onDesign.length === 1 && W.lab.onDesign[0].str === 'The Smiths' && W.lab.onDesign[0].on === 'HOUSE_1', 'the words on the labelled house are its live text: ' + JSON.stringify(W.lab.onDesign));
    ok(!W.lab.orphans.some(o => o.sku === 'THE SMITHS') && W.lab.orphans.some(o => o.sku === 'THE JONESES'), 'not an orphan; words on a house WITHOUT a SKU still are (they may be its label)');
    ok(/1 word\(s\) written on 1 design\(s\).*“The Smiths” on HOUSE_1/.test(P.onDesignNote(W.lab.onDesign)), 'and the run says so: ' + P.onDesignNote(W.lab.onDesign));
    ok(W.lab.duplicates.length === 1 && W.lab.duplicates[0].sku === 'TWIN_1', 'the second drawing of TWIN_1 is a duplicate');
    const why = Pair.whereUnlabelled(W.g.charms, W.lab, []);
    ok(/its label repeats TWIN_1, which charm #\d+ has/.test(why[W.lab.duplicates[0].charmIndex] || ''), 'and says so where it lists the charms without a SKU: ' + why[W.lab.duplicates[0].charmIndex]); }

  // ── 4 · STANDARD SIZE
  { const big = 15 * PT, small = 9 * PT;
    const sheet = await scene({ page: [560, 260], bodies: [
        { shape: 'house', x: 30, y: 120, w: big, h: big, hole: false },                 // 15 mm, a ring above it
        { shape: 'house', x: 110, y: 120, w: big, h: big },                             // 15 mm, a hanging hole punched in it
        { shape: 'house', x: 190, y: 120, w: small, h: small },                         // 9 mm
        { shape: 'bar', x: 250, y: 120, w: 25 * PT, h: 6 * PT, hole: false },            // 25 x 6 mm: long and narrow
        { shape: 'house', x: 340, y: 120, w: big, h: big, hole: false },                // 15 mm, its label names a size
        { shape: 'house', x: 420, y: 120, w: big, h: big, hole: false }],               // 15 mm, the size rule off
      rings: [{ cx: 30 + big / 2, cy: 120 + big + 2.75, r: 3.25, inner: 1.85 }],
      labels: [{ text: 'Ring_1', cx: 30 + big / 2, top: 118 }, { text: 'Hole_1', cx: 110 + big / 2, top: 118 }, { text: 'Small_1', cx: 190 + small / 2, top: 118 }, { text: 'Bar_1', cx: 250 + 12.5 * PT, top: 118 }, { text: 'Sized_1 · L', cx: 340 + big / 2, top: 118 }, { text: 'Off_1', cx: 420 + big / 2, top: 118 }] });
    const W = await read(sheet), skip = new Set(Pair.rowsOf(W.g.charms).flat().map(c => c.index));
    const hoopsOf = c => P.findHoops(c, V).map(h => [+h.outer.r.toFixed(3), h.aperture ? +h.aperture.r.toFixed(3) : 0]);
    const fit = (sku, o) => { const c = W.bySku(sku), l = W.lab.labels.get(c.index); return { c, before: hoopsOf(c), detail: c.members.find(m => m.fill && !m.stroke), r: P.fitCharmSize(c, Object.assign({ skip, sized: [l].concat(l.extra || []).some(x => x.size) }, o)) }; };
    const ring = fit('RING_1');
    ok(ring.r.k && Math.abs(ring.r.fromMm - 15) < 0.05 && ring.r.toMm === 11.5 && Math.abs(longMm(ring.c.outline) - 11.5) < 0.02, 'a 15 mm body is drawn at 11.5 mm: ' + JSON.stringify(ring.r) + ' ' + longMm(ring.c.outline).toFixed(3));
    ok(JSON.stringify(hoopsOf(ring.c)) === JSON.stringify(ring.before) && JSON.stringify(ring.before) === '[[3.25,1.85]]', 'its ring keeps its own size: ' + JSON.stringify(hoopsOf(ring.c)));
    { const r = P.integrateRings(ring.c); ok(r.welded === 1 && !r.left.length && !r.bridged.length, 'and still touches the body, so it is welded as before: ' + JSON.stringify(r)); }
    { const det = ring.c.members.find(m => m.fill && !m.stroke), k = ring.r.k; ok(Math.abs((det.bbox[2] - det.bbox[0]) - (ring.detail.bbox[2] - ring.detail.bbox[0]) * k) < 0.01, 'the artwork on it is scaled with it'); }
    { const bytes = new Uint8Array(await P.buildSingleCharm(ring.c, W.parsed)), back = await P.parseSource(bytes, 'r.ai'), live = P.groupCharms(back, { minPt: 6 }).charms.filter(c => c.mergedInto == null);
      ok(live.length === 1, 'its design file reads back as one charm');
      const c = live[0], hoop = P.findHoops(c, V)[0] || null, detail = c.members.find(m => m.fill && !m.stroke), b = trueBox(c.outline);
      ok(Math.abs((b[2] - b[0]) * MM - 11.5) < 0.05, 'in the file the body is 11.5 mm wide: ' + ((b[2] - b[0]) * MM).toFixed(3));
      ok(detail && Math.abs((detail.bbox[2] - detail.bbox[0]) - 0.3 * big * ring.r.k) < 0.05, 'the copied artwork is written at the new size too (the file is not the cut line over the old drawing)');
      ok(!hoop || Math.abs(hoop.outer.r - 3.25) < 0.02, 'and its ring at its own'); }
    const hole = fit('HOLE_1');
    ok(hole.r.k && JSON.stringify(hoopsOf(hole.c)) === JSON.stringify(hole.before) && hole.before.length === 1, 'a hanging hole punched in the body keeps its size: ' + JSON.stringify([hole.before, hoopsOf(hole.c)]));
    { const h = P.findHoops(hole.c, V)[0], b = trueBox(hole.c.outline); ok(G.pointInPolys(h.cx, h.cy, G.flatten(hole.c.outline, 8)) && G.distToPolys(h.cx, h.cy, G.flatten(hole.c.outline, 8)) > h.outer.r, 'and stays inside it, clear of its edge'); void b; }
    ok(fit('SMALL_1').r.skipped === 'fits' && !W.bySku('SMALL_1').sizeXform, 'a 9 mm body is left as drawn');
    ok(fit('BAR_1').r.skipped === 'long and narrow', 'a 25 x 6 mm bar is long and narrow: left as drawn');
    ok(fit('SIZED_1').r.skipped === 'its label names a size', 'a design whose label names a size is that size');
    ok(fit('OFF_1', { maxMm: 0 }).r.skipped === 'off' && !W.bySku('OFF_1').sizeXform, 'max 0: every charm as drawn');
    { const W2 = await read(sheet), c = W2.bySku('OFF_1'); ok(P.fitCharmSize(c, { skip: new Set([c.index]) }).skipped === 'touches a body without a SKU (a possible pair)', 'a body the pair layer may build with another keeps its size'); }
    const note = P.sizeRuleNote([{ sku: 'RING_1', r: ring.r }, { sku: 'BAR_1', r: { skipped: 'long and narrow', fromMm: 25 } }, { sku: 'SMALL_1', r: { skipped: 'fits', fromMm: 9 } }], 11.5);
    ok(/^1 charm\(s\) longer than 11.5 mm were drawn at 11.5 mm .*RING_1 15\.0 · 1 over it left as drawn \(1 long and narrow\)$/.test(note), 'the run says what it did: ' + note);
    // the Master tab writes design files in a worker, from a list of the charm's fields: the matrix must be on it, or the file holds the scaled cut line over the drawing at its old size
    ok(/const charm=c=>Object\.fromEntries\(\[[^\]]*'sizeXform'/.test(fs.readFileSync(path.join(__dirname, '../../charm-nest-background.js'), 'utf8')), 'the worker hand-off carries the size matrix');
    ok(P.READER_VERSION >= 5, 'the reader version moved with the drawing'); }

  console.log(`master-beady-1023 OK · ${n} checks · labels under curves and between legs, the seam weld (and its two-ring control), words on a design, a design drawn twice, the standard size`);
})().catch(e => { console.error(e); process.exit(1); });
