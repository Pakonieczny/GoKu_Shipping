// MONOGRAMUI (Paul, 15 Oct 2026): three 26-letter ALPHABETS for the monogram tests, made offline from the vendored fonts, named exactly as Paul's file names them:
//   Monogram_Left_A … Monogram_Left_Z, Monogram_Mid_A … Monogram_Mid_Z, Monogram_Right_A … Monogram_Right_Z  (the library upper-cases a SKU: MONOGRAM_LEFT_A …)
// Each letter is a real outline with its counters (the glyph's contours, overlapping contours merged), drawn at the same em size in three different fonts (the middle one a bold sans, so it stands apart), and written as a one-charm
// PDF in the app's convention (black 0.5 pt closed paths, outer ring + one path per counter: CharmNestMonogram.toPdf is the writer). `master(st)` puts the 78 index entries and files into the fake site's Firestore / Storage
// exactly as seedMaster does for the other real drawings.
//   const A = require('./_monogramAlphabets.cjs');  const set = await A.make();  A.seed(srv.st, set);
// Paul's own fonts (extracted from his file) can stand in when a directory with LEFT.ttf, MIDDLE.ttf and RIGHT.ttf is named in MONO_FONTS (never committed): the quality pass of MONOGRAMUI.
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const opentype = require(path.join(root, 'vendor/opentype-1.3.4.min.js'));
const C = require(path.join(root, 'vendor/clipper-6.4.2.js'));
const M = require(path.join(root, 'charm-nest-monogram.js'));
const LETTERS = [...'ABCDEFGHIJKLMNOPQRSTUVWXYZ'];
const MM = 25.4 / 72, SC = 1000;
const FONTS = { left: 'CrimsonText-SemiBold.ttf', middle: 'LibreFranklin-Bold.ttf', right: 'AlegreyaSans-Bold.ttf' };
const WORD = { left: 'Left', middle: 'Mid', right: 'Right' };

const loadFont = file => { const b = fs.readFileSync(file); return opentype.parse(b.buffer.slice(b.byteOffset, b.byteOffset + b.length)); };
/** A letter's rings in points (y up), em points tall, overlapping contours merged: [{ pts, hole }] with the outer ring(s) first. */
function glyphRings(font, ch, em) {
  const g = font.charToGlyph(ch), k = em / font.unitsPerEm, rings = []; let cur = null, last = null;
  const quad = (p0, c, p1) => { const o = []; for (let i = 1; i <= 10; i++) { const t = i / 10, u = 1 - t; o.push([u * u * p0[0] + 2 * u * t * c[0] + t * t * p1[0], u * u * p0[1] + 2 * u * t * c[1] + t * t * p1[1]]); } return o; };
  const cub = (p0, a, b, p1) => { const o = []; for (let i = 1; i <= 10; i++) { const t = i / 10, u = 1 - t; o.push([u * u * u * p0[0] + 3 * u * u * t * a[0] + 3 * u * t * t * b[0] + t * t * t * p1[0], u * u * u * p0[1] + 3 * u * u * t * a[1] + 3 * u * t * t * b[1] + t * t * t * p1[1]]); } return o; };
  for (const c of g.path.commands) {
    if (c.type === 'M') { cur = [[c.x, c.y]]; last = [c.x, c.y]; } else if (c.type === 'L') { cur.push([c.x, c.y]); last = [c.x, c.y]; }
    else if (c.type === 'Q') { cur.push(...quad(last, [c.x1, c.y1], [c.x, c.y])); last = [c.x, c.y]; } else if (c.type === 'C') { cur.push(...cub(last, [c.x1, c.y1], [c.x2, c.y2], [c.x, c.y])); last = [c.x, c.y]; }
    else if (c.type === 'Z') { if (cur && cur.length > 2) rings.push(cur); cur = null; }
  }
  if (cur && cur.length > 2) rings.push(cur);
  const cl = new C.Clipper(0), sol = new C.Paths();
  cl.AddPaths(rings.map(r => r.map(p => ({ X: Math.round(p[0] * k * SC), Y: Math.round(p[1] * k * SC) }))), C.PolyType.ptSubject, true);
  cl.Execute(C.ClipType.ctUnion, sol, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero);
  const out = sol.map(r => ({ pts: r.map(q => [q.X / SC, q.Y / SC]), hole: C.Clipper.Area(r) < 0 }));
  return out.sort((a, b) => (a.hole - b.hole) || Math.abs(shoelace(b.pts)) - Math.abs(shoelace(a.pts)));
}
const shoelace = pts => pts.reduce((a, p, i) => a + (p[0] * pts[(i + 1) % pts.length][1] - pts[(i + 1) % pts.length][0] * p[1]), 0) / 2;

/** One letter as a master file + its index entry. */
async function letterFile(font, ch, em, sku) {
  const rings = glyphRings(font, ch, em);
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const r of rings) for (const p of r.pts) { x0 = Math.min(x0, p[0]); y0 = Math.min(y0, p[1]); x1 = Math.max(x1, p[0]); y1 = Math.max(y1, p[1]); }
  const moved = rings.map(r => ({ pts: r.pts.map(p => [p[0] - x0, p[1] - y0]), hole: r.hole }));
  const w = x1 - x0, h = y1 - y0;
  const bytes = await M.toPdf({ ok: true, rings: moved, bboxPt: [0, 0, w, h] }, { title: sku });
  const holes = moved.filter(r => r.hole).length, area = moved.reduce((a, r) => a + shoelace(r.pts) * (r.hole ? -1 : 1) * (r.hole ? 1 : 1), 0);
  const metrics = { baselinePt: +(-y0).toFixed(3), emPt: em };   // (the baseline is y = 0 of the font: its height above the bottom of the outline is -y0; the same shape as the glyph metrics MONOGRAMFILE writes into Paul's per-SKU files)
  return { bytes, metrics, entry: { widthPt: +w.toFixed(3), heightPt: +h.toFixed(3), areaPt2: Math.round(Math.abs(moved.reduce((a, r) => a + Math.abs(shoelace(r.pts)) * (r.hole ? -1 : 1), 0))), members: moved.length, holes, engravable: false, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: (Math.abs(Math.round(area * 1000)) % 0xffffff).toString(16).padStart(6, '0'), backKeepOut: [] }, rings: moved, w, h };
}

/** A letter made of rings the way a converted FONT leaves it: the loop and a tail, two outer rings that cross each other (same direction), plus one counter: the overlap is solid ink, a counter is a ring of the other direction
 *  (the real Mid Q of Paul's file is three such rings). Written in the app's convention, one stroked closed path each. */
async function crossingLetterFile(sku) {
  const ell = (cx, cy, rx, ry, dir) => Array.from({ length: 72 }, (_, i) => { const t = dir * (i / 72) * 2 * Math.PI; return [cx + rx * Math.cos(t), cy + ry * Math.sin(t)]; });
  const rotate = (pts, deg, cx, cy) => pts.map(([x, y]) => { const a = deg * Math.PI / 180, dx = x - cx, dy = y - cy; return [cx + dx * Math.cos(a) - dy * Math.sin(a), cy + dx * Math.sin(a) + dy * Math.cos(a)]; });
  const loop = ell(20, 28, 15, 21, 1), counter = ell(20, 28, 7, 12, -1), tail = rotate(ell(30, 12, 11, 3, 1), 40, 30, 12);
  const rings = [{ pts: loop, hole: false }, { pts: tail, hole: false }, { pts: counter, hole: true }];
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const r of rings) for (const p of r.pts) { x0 = Math.min(x0, p[0]); y0 = Math.min(y0, p[1]); x1 = Math.max(x1, p[0]); y1 = Math.max(y1, p[1]); }
  const moved = rings.map(r => ({ pts: r.pts.map(p => [p[0] - x0, p[1] - y0]), hole: r.hole })), w = x1 - x0, h = y1 - y0;
  const bytes = await M.toPdf({ ok: true, rings: moved, bboxPt: [0, 0, w, h] }, { title: sku });
  return { bytes, w, h, entry: { widthPt: +w.toFixed(3), heightPt: +h.toFixed(3), areaPt2: 900, members: 3, holes: 1, engravable: false, engravableBy: 'index', upAngle: 90, upSource: 'drawn', charmHash: 'c055ed', backKeepOut: [] } };
}

/** All three alphabets. opts: { em (pt, default 53), fonts: { left, middle, right } file paths, skip: ['MONOGRAM_MID_Q'] letters left out }. */
async function make(opts = {}) {
  const em = opts.em || 53, fontDir = process.env.MONO_FONTS || '', files = new Map(), entries = [], letters = {};
  for (const slot of ['left', 'middle', 'right']) {
    const file = (opts.fonts && opts.fonts[slot]) || (fontDir ? path.join(fontDir, slot === 'left' ? 'LEFT.ttf' : slot === 'middle' ? 'MIDDLE.ttf' : 'RIGHT.ttf') : path.join(root, 'vendor/fonts', FONTS[slot]));
    const font = loadFont(file); letters[slot] = {};
    for (const L of LETTERS) {
      const raw = `Monogram_${WORD[slot]}_${L}`, sku = raw.toUpperCase();
      if ((opts.skip || []).includes(sku)) continue;
      const f = await letterFile(font, L, em, sku), aiPath = `charmnest/master/${raw}.ai`;
      files.set(aiPath, f.bytes); entries.push(Object.assign({ sku, aiPath }, f.entry)); letters[slot][L] = { sku, rings: f.rings, w: f.w, h: f.h, metrics: f.metrics };
    }
  }
  return { files, entries, letters, em };
}
/** Put the alphabets into the fake site (before the page reads the master). */
function seed(st, set) {
  for (const e of set.entries) st.put('Charm_Master_Index', e.sku, Object.assign({}, e, { thumbPath: null, blocked: null, sizes: null, conflict: null }));
  for (const [p, buf] of set.files) st.blobs.set(p, { buf: Buffer.from(buf), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });
}
module.exports = { make, seed, glyphRings, crossingLetterFile, LETTERS, FONTS };
