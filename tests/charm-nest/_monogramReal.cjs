// Paul's REAL 78-letter alphabet file (fixtures/SKUs_MONOGRAM_2024-1015.ai) read the way the page reads a letter, for the node-only monogram tests (MU4, 16 Oct 2026):
//   const R = await require('./_monogramReal.cjs').load();   R.skus (78 upper-case SKUs), R.real('left'|'middle'|'right', 'A') -> { letter, rings, sku, fill, metrics }, R.word('APV') -> { left, middle, right },
//   R.fileOf(sku) -> the per-SKU master file bytes (what the library stores), R.without(sku) -> a SKU list with one letter missing, R.masterCharm(entry) -> { parsed, charms: [charm] } (what Pool.masterCharm gives).
//   R.sound(result, { scale }) -> the list of structural problems of a finished monogram ([] = sound): built, finite numbers only, ONE outer ring, connected, every counter inside it, bbox at 0,0, 14.8 mm tall, report.metrics.
// A letter is read from its own single-charm master file (P.buildSingleCharm -> P.parseSource), exactly as the page's glyph() does (charm-nest-monogram-ui.js).
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { CharmNestPDF: P, Geom } = require(path.join(root, 'netlify/functions/_charmNestPdf.js'));
const FILE = path.join(__dirname, 'fixtures', 'SKUs_MONOGRAM_2024-1015.ai');
const SLOT = { left: 'LEFT', middle: 'MID', right: 'RIGHT' };
const MM = 25.4 / 72;
const NOMINAL_H = 14.8, H_TOL = 0.5;       // (the engine's pieces measure 14.77 to 15.2 mm over the 50 + 20 + 26 words of the sweep: "14.8 mm tall" within half a millimetre)

async function load(file) {
  const bytes = new Uint8Array(fs.readFileSync(file || FILE));
  const parsed = await P.parseSource(bytes, 'monogram.ai'), g = P.groupCharms(parsed, { minPt: 6 });
  const lab = P.labelCharms(parsed, g.charms, { pattern: P.SKU_PATTERN_DEFAULT, gapPt: 18.14, widen: 0.25 });
  const bySku = new Map([...lab.labels].map(([i, l]) => [l.sku, g.charms.find(c => c.index === i)])), cache = {}, files = new Map();
  const fileOf = async sku => {
    if (files.has(sku)) return files.get(sku);
    const c = bySku.get(sku); if (!c) throw new Error(sku + ' is not in the file');
    P.integrateRings(c); const b = new Uint8Array(await P.buildSingleCharm(c, parsed)); files.set(sku, b); return b;
  };
  // the biggest piece of a letter's own file with the loose bits that belong to it (a Q's tail), as the page keeps it
  const charmOf = async sku => {
    const p2 = await P.parseSource(await fileOf(sku), sku + '.ai'), live = P.groupCharms(p2, { minPt: 6 }).charms.filter(x => x.mergedInto == null);
    const area = x => (x.bbox[2] - x.bbox[0]) * (x.bbox[3] - x.bbox[1]), big = live.reduce((a, b) => (area(b) > area(a) ? b : a));
    for (const x of live) if (x !== big) for (const m of x.members) if (!big.members.includes(m)) big.members.push(m);
    return { parsed: p2, charms: [big] };
  };
  const real = async (slot, L) => {
    const sku = `MONOGRAM_${SLOT[slot] || slot}_${L}`; if (cache[sku]) return cache[sku];
    const src = await charmOf(sku), big = src.charms[0];
    const rings = Geom.flatten(big.outline, 16).filter(r => r.length > 2); for (const m of P.cutLinesOf(big)) for (const r of Geom.flatten(m, 16)) if (r.length > 2) rings.push(r);
    const gm = src.parsed.glyph;
    return (cache[sku] = { letter: L, rings, sku, fill: 'auto', metrics: gm && Number.isFinite(gm.baselinePt) ? { baselinePt: gm.baselinePt, emPt: gm.emPt } : null });
  };
  const word = async w => ({ left: await real('left', w[0]), middle: await real('middle', w[1]), right: await real('right', w[2]) });
  const skus = [...bySku.keys()].sort();
  return { skus, real, word, fileOf, bySku, charmOf, masterCharm: e => charmOf(e.sku), without: sku => skus.filter(s => s !== sku) };
}

const shoelace = pts => pts.reduce((a, p, i) => a + (p[0] * pts[(i + 1) % pts.length][1] - pts[(i + 1) % pts.length][0] * p[1]), 0) / 2;
const inPoly = (pt, poly) => { let c = false; for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) if ((poly[i][1] > pt[1]) !== (poly[j][1] > pt[1]) && pt[0] < (poly[j][0] - poly[i][0]) * (pt[1] - poly[i][1]) / (poly[j][1] - poly[i][1]) + poly[i][0]) c = !c; return c; };
/** The structural promises of every finished monogram. o.scale: the piece was fitted to a size (its height is the nominal one times the scale, never more). -> [] when sound. */
function sound(r, o) {
  o = o || {}; const bad = [];
  if (!r || r.ok !== true) return ['not built' + (r && r.why ? ': ' + r.why : '')];
  const nums = [r.wMm, r.hMm].concat(r.bboxPt || []); for (const g of r.rings || []) for (const p of g.pts) nums.push(p[0], p[1]);
  if (!(r.rings || []).length || !nums.every(Number.isFinite)) bad.push('NaN, Infinity or no geometry');
  const outer = (r.rings || []).filter(g => !g.hole);
  if (outer.length !== 1 || r.rings[0].hole) bad.push(`${outer.length} outer rings, not one piece`);
  if (!r.report || r.report.connected !== true) bad.push('report.connected is not true');
  if (!r.report || !r.report.metrics || !Array.isArray(r.report.metrics.violations)) bad.push('report.metrics is missing');
  if (bad.length) return bad;
  if (shoelace(outer[0].pts) <= 0) bad.push('the outer ring does not run counter-clockwise');
  if (r.rings.filter(g => g.hole).some(h => !inPoly(h.pts[0], outer[0].pts))) bad.push('a counter lies outside the outline');
  if (r.bboxPt[0] !== 0 || r.bboxPt[1] !== 0 || Math.abs(r.wMm - r.bboxPt[2] * MM) > 2e-3 || Math.abs(r.hMm - r.bboxPt[3] * MM) > 2e-3) bad.push('bbox is not 0,0 to wMm x hMm');
  const want = NOMINAL_H * (o.scale || 1), tol = H_TOL * (o.scale || 1);
  if (Math.abs(r.hMm - want) > tol) bad.push(`${r.hMm.toFixed(2)} mm tall, not ${want.toFixed(1)} +- ${tol.toFixed(2)}`);
  if (!(r.wMm > 8 && r.wMm < 40)) bad.push(`${r.wMm.toFixed(1)} mm wide`);
  return bad;
}

/** The cut-file preflight of a custom design: the same steps as CustomSheet.read + ingest (charm-nest-bridge.js: parse, group, loose ink joins its piece, hoops joined, closed pieces only, the size a piece is cut at; not the canvas silhouettes).
 *  -> { pieces, wMm, hMm } or throws the message the Designs window would show. (The real CustomSheet runs in monogram-flow.cjs; this is its node twin for the file alone.) */
function outlineBox(seg) {
  const b = [Infinity, Infinity, -Infinity, -Infinity], at = p => { if (p[0] < b[0]) b[0] = p[0]; if (p[1] < b[1]) b[1] = p[1]; if (p[0] > b[2]) b[2] = p[0]; if (p[1] > b[3]) b[3] = p[1]; };
  for (const sub of seg.subpaths || []) {
    let cur = null;
    for (const sg of sub) {
      if (sg[0] === 'm' || sg[0] === 'l') { at(sg[1]); cur = sg[1]; }
      else if (sg[0] === 'c' && cur) { const [a, c2, e] = [sg[1], sg[2], sg[3]]; for (let i = 1; i <= 24; i++) { const t = i / 24, u = 1 - t; at([u * u * u * cur[0] + 3 * u * u * t * a[0] + 3 * u * t * t * c2[0] + t * t * t * e[0], u * u * u * cur[1] + 3 * u * u * t * a[1] + 3 * u * t * t * c2[1] + t * t * t * e[1]]); } cur = e; }
    }
  }
  return Number.isFinite(b[0]) ? b : seg.bbox;
}
async function preflight(bytes, name) {
  const parsed = await P.parseSource(bytes, name);
  const g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: 6 });
  if (!g.charms.length) throw new Error('no closed cut outline in it — a piece\'s outline must be a closed black or grey line');
  for (const m of g.orphans) {
    if (!m.bbox) continue;
    const c = g.charms.find(c => { const b = c.bbox, pad = 2; return m.bbox[0] >= b[0] - pad && m.bbox[1] >= b[1] - pad && m.bbox[2] <= b[2] + pad && m.bbox[3] <= b[3] + pad; });
    if (c && !c.members.includes(m)) c.members.push(m);
  }
  for (const c of g.charms) { const r = P.integrateRings(c); if (r.left.length) throw new Error('a hoop could not join its piece — ' + r.left[0]); }
  // (the silhouettes the sheet nests with are drawn on a canvas: that step is the browser's, monogram-flow.cjs runs the real one)
  const charms = g.charms.filter(c => !c.open);
  if (!charms.length) throw new Error('its outlines are not closed — every piece needs a closed cut line');
  const bb = charms.reduce((a, c) => { const o = outlineBox(c.outline); return [Math.min(a[0], o[0]), Math.min(a[1], o[1]), Math.max(a[2], o[2]), Math.max(a[3], o[3])]; }, [Infinity, Infinity, -Infinity, -Infinity]);
  return { pieces: charms.length, wMm: (bb[2] - bb[0]) * MM, hMm: (bb[3] - bb[1]) * MM, charms, parsed };
}
module.exports = { load, sound, preflight, outlineBox, FILE, MM, NOMINAL_H, H_TOL, shoelace, inPoly };
