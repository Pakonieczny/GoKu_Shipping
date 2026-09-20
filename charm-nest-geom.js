/*  charm-nest-geom.js — the back of a charm, checked; text fitted on solid material.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Pure JavaScript, no canvas, no DOM: the same file runs in the sorter page,
 *  in the Netlify functions and in the node tests, so every number the
 *  reviewer sees can be reproduced outside a browser.
 *
 *    backView(charm, opts)         mirror the cut geometry, verify the flip step by step, orient hoop-up (design §7.2)
 *    upAngleOf(charm, opts)        direction from the outline's centroid to its hanging hole (§6.3 item 7)
 *    engraveMask(view, opts)       the back silhouette eroded by the engraving margin, keep-outs subtracted (§7.3)
 *    fitText(lines, font, mask, o) the largest Myriad Pro size whose ink sits entirely on solid material (§7.3)
 *    refitAt(...)                  the same, at a fixed centre (nudge)
 *    glyphCoverage(font, text)     characters the font lacks (§7.1 exactness)
 *    verifyInk(glyphs, mask)       the hard check: zero ink outside the eroded mask (§7.4)
 *    raster, erode, largestRectangles, rotateMask, flipX, diffFraction, area — the primitives
 *
 *  Frames. Every mask is a pixel grid over a y-up pt frame: pixel (px, py)
 *  covers x = ox + (px + .5) / res, y = oy + (py + .5) / res. A mask is
 *  built symmetric about its mirror axis cx, so mirroring the geometry and
 *  mirroring the pixel columns land on the same grid — that is what makes
 *  "B == flipX(F)" a pixel-for-pixel test rather than an approximation.
 *
 *  Glyph paths come from opentype.js (y down) and are flipped into the y-up
 *  frame here; nothing downstream sees a font, only paths.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.CharmNestGeom = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";
  const MM_PER_PT = 25.4 / 72, PT_PER_MM = 72 / 25.4;

  /* ═══ 1 · matrices and segments ════════════════════════════════════════ */
  const mul = (m, k) => [m[0] * k[0] + m[1] * k[2], m[0] * k[1] + m[1] * k[3], m[2] * k[0] + m[3] * k[2], m[2] * k[1] + m[3] * k[3], m[4] * k[0] + m[5] * k[2] + k[4], m[4] * k[1] + m[5] * k[3] + k[5]];
  const ap = (m, x, y) => [m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5]];
  const scaleOf = m => Math.sqrt(Math.abs(m[0] * m[3] - m[1] * m[2])) || 1;
  const mirrorX = cx => [-1, 0, 0, 1, 2 * cx, 0];
  const rotateAbout = (cx, cy, deg) => { const t = deg * Math.PI / 180, c = Math.cos(t), s = Math.sin(t); return [c, s, -s, c, cx - cx * c + cy * s, cy - cx * s - cy * c]; };
  const translate = (tx, ty) => [1, 0, 0, 1, tx, ty];
  const bboxOf = pts => { let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const p of pts) { if (p[0] < x0) x0 = p[0]; if (p[0] > x1) x1 = p[0]; if (p[1] < y0) y0 = p[1]; if (p[1] > y1) y1 = p[1]; } return pts.length ? [x0, y0, x1, y1] : null; };
  const bbUnion = (a, b) => !a ? b : !b ? a : [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])];

  /** A segment with every point and Bézier handle mapped through M. */
  function transformSeg(seg, M) {
    const P = p => ap(M, p[0], p[1]);
    const subpaths = (seg.subpaths || []).map(sub => sub.map(s => s[0] === "m" || s[0] === "l" ? [s[0], P(s[1])] : s[0] === "c" ? ["c", P(s[1]), P(s[2]), P(s[3])] : s.slice()));
    const pts = []; for (const sub of subpaths) for (const s of sub) for (let i = 1; i < s.length; i++) pts.push(s[i]);
    return Object.assign({}, seg, { subpaths, bbox: bboxOf(pts), lwPt: (seg.lwPt || 0) * scaleOf(M), transformed: true, original: seg.original || seg });
  }
  /** Flatten a segment's subpaths to polylines (closed for rasterising). */
  function flatten(seg, steps) {
    steps = steps || 12; const polys = [];
    for (const sub of seg.subpaths || []) {
      let poly = [], cur = null;
      for (const sg of sub) {
        if (sg[0] === "m") { if (poly.length > 1) polys.push(poly); poly = [sg[1]]; cur = sg[1]; }
        else if (sg[0] === "l") { poly.push(sg[1]); cur = sg[1]; }
        else if (sg[0] === "c" && cur) { const [a, b, c] = [sg[1], sg[2], sg[3]]; for (let i = 1; i <= steps; i++) { const t = i / steps, u = 1 - t; poly.push([u * u * u * cur[0] + 3 * u * u * t * a[0] + 3 * u * t * t * b[0] + t * t * t * c[0], u * u * u * cur[1] + 3 * u * u * t * a[1] + 3 * u * t * t * b[1] + t * t * t * c[1]]); } cur = c; }
      }
      if (poly.length > 1) polys.push(poly);
    }
    return polys;
  }
  /** Polygon centroid (area weighted) and signed area. */
  function polyCentroid(poly) {
    let a = 0, cx = 0, cy = 0;
    for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { const f = poly[j][0] * poly[i][1] - poly[i][0] * poly[j][1]; a += f; cx += (poly[j][0] + poly[i][0]) * f; cy += (poly[j][1] + poly[i][1]) * f; }
    if (Math.abs(a) < 1e-9) { const b = bboxOf(poly); return { x: (b[0] + b[2]) / 2, y: (b[1] + b[3]) / 2, area: 0 }; }
    return { x: cx / (3 * a), y: cy / (3 * a), area: a / 2 };
  }
  function pointInPolys(x, y, polys) {
    let inside = false;
    for (const poly of polys) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
      const xi = poly[i][0], yi = poly[i][1], xj = poly[j][0], yj = poly[j][1];
      if ((yi > y) !== (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) inside = !inside;
    }
    return inside;
  }
  function distToPolys(x, y, polys) {
    let best = Infinity;
    for (const poly of polys) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
      const ax = poly[j][0], ay = poly[j][1], bx = poly[i][0], by = poly[i][1];
      const dx = bx - ax, dy = by - ay, L = dx * dx + dy * dy;
      const t = L ? Math.max(0, Math.min(1, ((x - ax) * dx + (y - ay) * dy) / L)) : 0;
      const px = ax + t * dx - x, py = ay + t * dy - y; const d = px * px + py * py; if (d < best) best = d;
    }
    return Math.sqrt(best);
  }
  /** A point guaranteed inside a polygon: the midpoint of the longest even-odd span on the row through its centroid. */
  function interiorPoint(polys) {
    const c = polyCentroid(polys[0]);
    const y = c.y; const xs = [];
    for (const poly of polys) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { const yi = poly[i][1], yj = poly[j][1]; if ((yi > y) !== (yj > y)) xs.push(poly[j][0] + (y - yj) * (poly[i][0] - poly[j][0]) / (yi - yj)); }
    xs.sort((a, b) => a - b);
    let best = null;
    for (let k = 0; k + 1 < xs.length; k += 2) { const len = xs[k + 1] - xs[k]; if (!best || len > best.len) best = { len, x: (xs[k] + xs[k + 1]) / 2 }; }
    return best ? [best.x, y] : [c.x, c.y];
  }

  /* ═══ 2 · masks ════════════════════════════════════════════════════════ */
  /** A grid symmetric about `cx` covering `bbox` with `padPt` around it. */
  function makeFrame(bbox, res, cx, padPt) {
    padPt = padPt == null ? 1 : padPt;
    const halfW = Math.max(cx - bbox[0], bbox[2] - cx) + padPt;
    const w = Math.max(2, Math.ceil(2 * halfW * res)), h = Math.max(2, Math.ceil((bbox[3] - bbox[1] + 2 * padPt) * res));
    return { w, h, res, ox: cx - w / (2 * res), oy: bbox[1] - padPt, cx, cy: (bbox[1] + bbox[3]) / 2 };
  }
  const emptyMask = frame => Object.assign({}, frame, { bits: new Uint8Array(frame.w * frame.h) });
  const cloneMask = m => Object.assign({}, m, { bits: m.bits.slice() });
  /** Even-odd scanline fill of polygons (pt) into the frame. Returns a mask. */
  function rasterPolys(polys, frame, into, value) {
    const m = into || emptyMask(frame); const { w, h, res, ox, oy } = m; const bits = m.bits; value = value == null ? 1 : value;
    const edges = [];
    for (const poly of polys) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { const a = poly[j], b = poly[i]; if (a[1] === b[1]) continue; edges.push(a[1] < b[1] ? [a[0], a[1], b[0], b[1]] : [b[0], b[1], a[0], a[1]]); }
    if (!edges.length) return m;
    edges.sort((p, q) => p[1] - q[1]);
    const xs = new Float64Array(edges.length);
    for (let py = 0; py < h; py++) {
      const y = oy + (py + 0.5) / res; let n = 0;
      for (const e of edges) { if (e[1] > y) break; if (e[3] <= y) continue; xs[n++] = e[0] + (y - e[1]) * (e[2] - e[0]) / (e[3] - e[1]); }
      if (n < 2) continue;
      const row = Array.prototype.slice.call(xs, 0, n).sort((a, b) => a - b);
      for (let k = 0; k + 1 < n; k += 2) {
        // a 1e-6 px tolerance makes exact-tie intersections (integer artwork on 45° edges) land the same way after a mirror
        let x0 = Math.ceil((row[k] - ox) * res - 0.5 - 1e-6), x1 = Math.floor((row[k + 1] - ox) * res - 0.5 + 1e-6);
        if (x0 < 0) x0 = 0; if (x1 >= w) x1 = w - 1;
        for (let px = x0; px <= x1; px++) bits[py * w + px] = value;
      }
    }
    return m;
  }
  const polysOfSegs = (segs, steps) => segs.flatMap(s => flatten(s, steps));
  function raster(segs, frame) { return rasterPolys(polysOfSegs(segs), frame); }
  function area(m) { let n = 0; const b = m.bits; for (let i = 0; i < b.length; i++) n += b[i]; return n; }
  function flipX(m) { const out = cloneMask(m); const { w, h } = m; for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) out.bits[y * w + x] = m.bits[y * w + (w - 1 - x)]; return out; }
  function diffFraction(a, b) { if (a.w !== b.w || a.h !== b.h) return 1; let d = 0; for (let i = 0; i < a.bits.length; i++) if (a.bits[i] !== b.bits[i]) d++; const denom = Math.max(1, area(a), area(b)); return d / denom; }
  function at(m, x, y) { const px = Math.floor((x - m.ox) * m.res), py = Math.floor((y - m.oy) * m.res); if (px < 0 || py < 0 || px >= m.w || py >= m.h) return 0; return m.bits[py * m.w + px]; }
  /** Exact Euclidean distance transform (Felzenszwalb): for every pixel, the distance in px to the nearest pixel where `isSite` is true. */
  function distanceTransform(bits, w, h, isSite) {
    const INF = 1e12; const f = new Float64Array(w * h);
    for (let i = 0; i < w * h; i++) f[i] = isSite(bits[i]) ? 0 : INF;
    const tmp = new Float64Array(Math.max(w, h)), v = new Int32Array(Math.max(w, h)), z = new Float64Array(Math.max(w, h) + 1);
    const dt1 = (get, set, n) => {
      let k = 0; v[0] = 0; z[0] = -INF; z[1] = INF;
      for (let q = 1; q < n; q++) {
        let s = ((get(q) + q * q) - (get(v[k]) + v[k] * v[k])) / (2 * q - 2 * v[k]);
        while (s <= z[k]) { k--; s = ((get(q) + q * q) - (get(v[k]) + v[k] * v[k])) / (2 * q - 2 * v[k]); }
        k++; v[k] = q; z[k] = s; z[k + 1] = INF;
      }
      k = 0; for (let q = 0; q < n; q++) { while (z[k + 1] < q) k++; tmp[q] = (q - v[k]) * (q - v[k]) + get(v[k]); }
      for (let q = 0; q < n; q++) set(q, tmp[q]);
    };
    for (let x = 0; x < w; x++) dt1(y => f[y * w + x], (y, val) => { f[y * w + x] = val; }, h);
    for (let y = 0; y < h; y++) dt1(x => f[y * w + x], (x, val) => { f[y * w + x] = val; }, w);
    const out = new Float32Array(w * h); for (let i = 0; i < w * h; i++) out[i] = Math.sqrt(f[i]);
    return out;
  }
  /** Keep only pixels farther than `rPx` from any empty pixel (and from the frame edge). */
  function erode(m, rPx) {
    const out = cloneMask(m); if (!(rPx > 0)) return out;
    const { w, h } = m; const dt = distanceTransform(m.bits, w, h, v => v === 0);
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { const i = y * w + x; const edge = Math.min(x + 1, y + 1, w - x, h - y); out.bits[i] = (m.bits[i] && dt[i] > rPx && edge > rPx) ? 1 : 0; }
    return out;
  }
  function subtract(m, polys) { const out = cloneMask(m); rasterPolys(polys, m, out, 0); return out; }
  /** Nearest-neighbour rotation of a mask about its (cx, cy) into a fresh symmetric frame. For the SEARCH only; the final check is on the unrotated mask. */
  function rotateMask(m, deg) {
    if (!deg) return cloneMask(m);
    const R = rotateAbout(m.cx, m.cy, deg), Ri = rotateAbout(m.cx, m.cy, -deg);
    const corners = [[m.ox, m.oy], [m.ox + m.w / m.res, m.oy], [m.ox, m.oy + m.h / m.res], [m.ox + m.w / m.res, m.oy + m.h / m.res]].map(p => ap(R, p[0], p[1]));
    const frame = makeFrame(bboxOf(corners), m.res, m.cx, 0); frame.cy = m.cy;
    const out = emptyMask(frame);
    for (let py = 0; py < out.h; py++) for (let px = 0; px < out.w; px++) {
      const x = out.ox + (px + 0.5) / out.res, y = out.oy + (py + 0.5) / out.res;
      const s = ap(Ri, x, y); out.bits[py * out.w + px] = at(m, s[0], s[1]);
    }
    return out;
  }
  /** Largest axis-aligned rectangles of 1s (histogram/stack), best first; each one is cleared before the next search. */
  function largestRectangles(m, n) {
    const work = m.bits.slice(); const { w, h, res, ox, oy } = m; const out = [];
    const hgt = new Int32Array(w), stack = new Int32Array(w + 1);
    for (let k = 0; k < (n || 6); k++) {
      hgt.fill(0); let best = { area: 0 };
      for (let y = 0; y < h; y++) {
        for (let x = 0; x < w; x++) hgt[x] = work[y * w + x] ? hgt[x] + 1 : 0;
        let sp = 0;
        for (let x = 0; x <= w; x++) {
          const cur = x < w ? hgt[x] : 0;
          while (sp > 0 && hgt[stack[sp - 1]] >= cur) { const hh = hgt[stack[--sp]]; const left = sp > 0 ? stack[sp - 1] + 1 : 0; const ww = x - left, a = ww * hh; if (a > best.area) best = { area: a, x: left, y: y - hh + 1, w: ww, h: hh }; }
          stack[sp++] = x;
        }
      }
      if (!best.area || best.w < 2 || best.h < 2) break;
      out.push({ px: best.x, py: best.y, pw: best.w, ph: best.h, x0: ox + best.x / res, y0: oy + best.y / res, x1: ox + (best.x + best.w) / res, y1: oy + (best.y + best.h) / res, wPt: best.w / res, hPt: best.h / res, areaPt2: best.area / (res * res) });
      for (let y = best.y; y < best.y + best.h; y++) for (let x = best.x; x < best.x + best.w; x++) work[y * w + x] = 0;
    }
    return out;
  }

  /* ═══ 3 · the back: an actual flip, verified ═══════════════════════════ */
  const achromatic = c => c && (Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2])) <= 0.15;
  const isCutLine = m => !!m && m.kind === "path" && !!m.stroke && !!m.closed && achromatic(m.strokeRGB);
  const FLIP_WHY = { pixels: "the flipped back does not match the front", holes: "a cut-out does not stay open when the charm is flipped", area: "the flipped back covers a different area", detailDropped: "front-only detail would show on the back" };
  class BackViewError extends Error { constructor(checks, images) { const bad = Object.keys(checks).filter(k => !checks[k]); super("flip check failed — " + bad.map(k => FLIP_WHY[k] || k).join("; ")); this.checks = checks; this.failed = bad; this.images = images; } }

  /** Direction (degrees, y-up, 90 = straight up) from the outline's centroid to the hanging hole. No hole → 90 (as drawn). */
  function upAngleOf(charm, opts) {
    opts = opts || {}; const cut = opts.isCut || isCutLine;
    const outlinePolys = flatten(charm.outline, 12);
    const c = polyCentroid(outlinePolys[0]);
    const holes = charm.members.filter(m => m !== charm.outline && cut(m));
    if (!holes.length) return { angle: 90, hole: null, source: "drawn" };
    let best = null;
    for (const h of holes) {
      const b = h.bbox; const size = (b[2] - b[0]) * (b[3] - b[1]);
      const p = interiorPoint(flatten(h, 12));
      const edge = distToPolys(p[0], p[1], outlinePolys);
      const score = edge + Math.sqrt(size) * 0.25;                 // smallest cut-out nearest the outline edge
      if (!best || score < best.score) best = { score, hole: h, p, edge, size };
    }
    const angle = Math.atan2(best.p[1] - c.y, best.p[0] - c.x) * 180 / Math.PI;
    return { angle: ((angle % 360) + 360) % 360, hole: best.hole, source: "hole", centroid: [c.x, c.y] };
  }

  /**
   * charm = { outline, members[] }  (the sorter's charm; members are parsed segments)
   * opts  = { res: 6, isCut, upAngle (deg, default from upAngleOf), tolPixels: 0.0005, tolArea: 0.001 }
   * Returns { members (mirrored + oriented cut geometry), mask (oriented back silhouette, holes open), cx, cy, M, R,
   *           angleDeg, upAngle, checks, F, B, frame }. Throws BackViewError with the diff images when a check fails.
   */
  function backView(charm, opts) {
    opts = Object.assign({ res: 6, tolPixels: 0.0005, tolArea: 0.001 }, opts || {});
    const cut = opts.isCut || isCutLine;
    const originalOutline = charm.outline;
    let outline = originalOutline;
    if (opts.solidBack && originalOutline.subpaths.length > 1) {
      // Explicit operator choice: expanded artwork can contain white front detail, not physical cut-outs.
      // Keep every outer island; separate stroked cut-outs are never removed by this choice.
      const polys = originalOutline.subpaths.map(sub=>flatten({subpaths:[sub]},24)[0]);
      const outer = originalOutline.subpaths.filter((sub,i)=>!polys.some((p,j)=>i!==j && Math.abs(polyCentroid(p).area)>Math.abs(polyCentroid(polys[i]).area) && pointInPolys(...interiorPoint([polys[i]]),[p])));
      outline = Object.assign({}, originalOutline, {subpaths:outer, synthetic:true, transformed:true, stroke:true, fill:false, strokeRGB:[0,0,0], paintOp:"S", start:-1, end:-1});
    }
    const cutMembers = charm.members.filter(m => m === originalOutline || cut(m)).map(m=>m===originalOutline ? outline : m);
    if (!cutMembers.includes(outline)) cutMembers.unshift(outline);
    const dropped = charm.members.filter(m => m !== originalOutline && !cutMembers.includes(m));
    const ob = charm.outline.bbox; const cx = (ob[0] + ob[2]) / 2, cy = (ob[1] + ob[3]) / 2;
    const bb = cutMembers.reduce((a, s) => bbUnion(a, s.bbox), null);
    const frame = makeFrame(bb, opts.res, cx, 1);
    const F = raster(cutMembers, frame);                                                         // STEP 2 · holes open (even-odd)
    const M = mirrorX(cx);                                                                       // STEP 3
    const mirrored = cutMembers.map(m => transformSeg(m, M));
    const B = raster(mirrored, frame);                                                           // STEP 4
    const holes = cutMembers.filter(m => m !== outline);
    const checks = {                                                                             // STEP 5
      pixels: diffFraction(B, flipX(F)) <= opts.tolPixels,
      holes: holes.every(h => { const p = interiorPoint(flatten(h, 12)); return at(B, 2 * cx - p[0], p[1]) === at(F, p[0], p[1]); }),
      area: Math.abs(area(B) - area(F)) / Math.max(1, area(F)) <= opts.tolArea,
      detailDropped: mirrored.every(m => cut(m) || m.original === outline) && !mirrored.some(m => m.fill && !m.stroke && m.original !== outline) && dropped.every(m => !cutMembers.includes(m))
    };
    const detail = { filledArtwork: outline !== originalOutline, pixelDiff: diffFraction(B, flipX(F)), areaF: area(F), areaB: area(B), dropped: dropped.length, cut: cutMembers.length };
    if (!Object.values(checks).every(Boolean)) throw new BackViewError(checks, { F, B, flipF: flipX(F), detail });
    const up = opts.upAngle != null ? +opts.upAngle : upAngleOf(Object.assign({}, charm, {outline, members:cutMembers}), { isCut: cut }).angle;    // STEP 6
    const angleDeg = 90 - up;
    const R = rotateAbout(cx, cy, angleDeg);
    const members = mirrored.map(m => transformSeg(m, R));
    const obb = members.reduce((a, s) => bbUnion(a, s.bbox), null);
    const oframe = makeFrame(obb, opts.res, cx, 1); oframe.cy = cy;
    const mask = raster(members, oframe);
    return { members, mask, cx, cy, M, R, angleDeg, upAngle: up, checks, detail, F, B, frame, cutMembers, dropped };
  }

  /** The eroded back mask: margin off every cut edge and cut-out, keep-out layers subtracted. */
  function engraveMask(view, opts) {
    opts = Object.assign({ marginMm: 0.8, keepOut: [] }, opts || {});
    let m = view.mask;
    if (opts.keepOut && opts.keepOut.length) m = subtract(m, polysOfSegs(opts.keepOut.map(k => transformSeg(transformSeg(k, view.M), view.R))));
    const rPx = opts.marginMm * PT_PER_MM * m.res;
    m = erode(m, rPx);
    m.marginMm = opts.marginMm; m.wPt = m.w / m.res; m.hPt = m.h / m.res;
    return m;
  }

  /* ═══ 4 · text ═════════════════════════════════════════════════════════ */
  /** Characters the font cannot set exactly. Newlines are line breaks, never characters. */
  function glyphCoverage(font, text) {
    if (font.coverage) return font.coverage(String(text || ""));
    const missing = [];
    for (const ch of String(text || "")) { if (ch === "\n" || ch === "\r") continue; let gi = 0; try { gi = font.charToGlyphIndex(ch); } catch (_) { gi = 0; } if (!gi && !missing.includes(ch)) missing.push(ch); }
    return { ok: !missing.length, missing };
  }
  const capPerEm = font => ((font.tables && font.tables.os2 && font.tables.os2.sCapHeight) || (font.ascender * 0.7)) / font.unitsPerEm;
  /** Glyph outlines of one line at `size`, y-up, origin at the baseline start. */
  function lineGlyphs(font, text, size) {
    const path = font.getPath(text, 0, 0, size, { kerning: true });
    const cmds = path.commands.map(c => c.type === "M" || c.type === "L" ? { type: c.type, x: c.x, y: -c.y } : c.type === "C" ? { type: "C", x: c.x, y: -c.y, x1: c.x1, y1: -c.y1, x2: c.x2, y2: -c.y2 } : c.type === "Q" ? { type: "Q", x: c.x, y: -c.y, x1: c.x1, y1: -c.y1 } : { type: "Z" });
    const bb = path.getBoundingBox();
    return { cmds, bbox: [bb.x1, -bb.y2, bb.x2, -bb.y1], advance: font.getAdvanceWidth(text, size, { kerning: true }) };
  }
  /** Lay lines out centred on the origin; returns glyph command lists in the local frame plus the ink bbox. */
  function layoutLines(lines, font, size, lineGap, angle, centre) {
    const cap = size * capPerEm(font), gap = size * (lineGap == null ? 0.18 : lineGap);
    const totalH = lines.length * cap + (lines.length - 1) * gap;
    const rows = lines.map((t, i) => { const g = lineGlyphs(font, t, size); const x = -g.advance / 2, y = totalH / 2 - cap - i * (cap + gap); return { text: t, x, y, glyphs: g, bbox: [g.bbox[0] + x, g.bbox[1] + y, g.bbox[2] + x, g.bbox[3] + y] }; });
    const local = rows.reduce((a, r) => bbUnion(a, r.bbox), null) || [0, 0, 0, 0];
    const R = mul(rotateAbout(0, 0, angle || 0), translate(centre ? centre[0] : 0, centre ? centre[1] : 0));
    const P = (x, y) => ap(R, x, y);
    const glyphs = rows.map(r => ({ text: r.text, cmds: r.glyphs.cmds.map(k => { const o = { type: k.type }; if (k.type !== "Z") { const p = P(k.x + r.x, k.y + r.y); o.x = p[0]; o.y = p[1]; } if (k.type === "C" || k.type === "Q") { const p1 = P(k.x1 + r.x, k.y1 + r.y); o.x1 = p1[0]; o.y1 = p1[1]; } if (k.type === "C") { const p2 = P(k.x2 + r.x, k.y2 + r.y); o.x2 = p2[0]; o.y2 = p2[1]; } return o; }) }));
    const corners = [[local[0], local[1]], [local[2], local[1]], [local[0], local[3]], [local[2], local[3]]].map(p => P(p[0], p[1]));
    return { size, angle: angle || 0, centre: centre || [0, 0], rows, local, bbox: bboxOf(corners), glyphs, capPt: cap, totalH, cmds: glyphs.flatMap(g => g.cmds) };
  }
  /** Glyph command lists → polygons (pt). Quadratics are flattened directly. */
  function glyphPolys(cmds, steps) {
    steps = steps || 8; const polys = []; let poly = [], cur = null;
    for (const k of cmds) {
      if (k.type === "M") { if (poly.length > 1) polys.push(poly); poly = [[k.x, k.y]]; cur = [k.x, k.y]; }
      else if (k.type === "L") { poly.push([k.x, k.y]); cur = [k.x, k.y]; }
      else if (k.type === "C" && cur) { for (let i = 1; i <= steps; i++) { const t = i / steps, u = 1 - t; poly.push([u * u * u * cur[0] + 3 * u * u * t * k.x1 + 3 * u * t * t * k.x2 + t * t * t * k.x, u * u * u * cur[1] + 3 * u * u * t * k.y1 + 3 * u * t * t * k.y2 + t * t * t * k.y]); } cur = [k.x, k.y]; }
      else if (k.type === "Q" && cur) { for (let i = 1; i <= steps; i++) { const t = i / steps, u = 1 - t; poly.push([u * u * cur[0] + 2 * u * t * k.x1 + t * t * k.x, u * u * cur[1] + 2 * u * t * k.y1 + t * t * k.y]); } cur = [k.x, k.y]; }
      else if (k.type === "Z") { if (poly.length > 1) polys.push(poly); poly = []; cur = null; }
    }
    if (poly.length > 1) polys.push(poly);
    return polys;
  }
  function rasterGlyphs(cmds, frame) {
    // Canvas and exported PDF fill glyphs with nonzero winding. Emoji can have overlapping components;
    // an even-odd mask would incorrectly erase their overlap during the manufacturing check.
    const m=emptyMask(frame), edges=[];
    for (const p of glyphPolys(cmds)) for (let i=0,j=p.length-1;i<p.length;j=i++) {
      const a=p[j],b=p[i]; if(a[1]===b[1])continue;
      edges.push(a[1]<b[1]?[a[0],a[1],b[0],b[1],1]:[b[0],b[1],a[0],a[1],-1]);
    }
    edges.sort((a,b)=>a[1]-b[1]);
    for(let py=0;py<m.h;py++) {
      const y=m.oy+(py+.5)/m.res,row=[];
      for(const e of edges){if(e[1]>y)break;if(e[3]<=y)continue;row.push([e[0]+(y-e[1])*(e[2]-e[0])/(e[3]-e[1]),e[4]]);}
      row.sort((a,b)=>a[0]-b[0]);let winding=0;
      for(let i=0;i+1<row.length;i++) {
        winding+=row[i][1];if(!winding)continue;
        const lo=Math.max(0,Math.ceil((row[i][0]-m.ox)*m.res-.5-1e-6)),hi=Math.min(m.w-1,Math.floor((row[i+1][0]-m.ox)*m.res-.5+1e-6));
        for(let x=lo;x<=hi;x++)m.bits[py*m.w+x]=1;
      }
    }
    return m;
  }
  /** The hard check: every ink pixel sits on a 1 of the mask. */
  function verifyInk(cmds, mask) {
    // Rasterization clips to its frame; reject off-frame vector ink first so
    // shrinking/searching cannot mistake clipped-away letters for a valid fit.
    const polys = glyphPolys(cmds), bb = bboxOf(polys.flat());
    if (!bb || bb[0] < mask.ox || bb[1] < mask.oy || bb[2] > mask.ox + mask.w / mask.res || bb[3] > mask.oy + mask.h / mask.res)
      return {ok:false, outside:1, total:0};
    // Fine lettering can fall between raster sample centres. Its outline must
    // also stay on solid mask pixels, including every segment between vertices.
    for (const p of polys) for (let i=0,j=p.length-1;i<p.length;j=i++) {
      const a=p[j], b=p[i], n=Math.max(1,Math.ceil(Math.hypot(b[0]-a[0],b[1]-a[1])*mask.res*2));
      for(let k=0;k<=n;k++) if(!at(mask,a[0]+(b[0]-a[0])*k/n,a[1]+(b[1]-a[1])*k/n)) return {ok:false,outside:1,total:0};
    }
    const ink = rasterGlyphs(cmds, mask); let outside = 0, total = 0;
    for (let i = 0; i < ink.bits.length; i++) if (ink.bits[i]) { total++; if (!mask.bits[i]) outside++; }
    return { ok: outside === 0 && total > 0, outside, total };
  }
  /** Thinnest stem and smallest inter-stroke gap of rendered ink, in mm, from 1-D runs in four directions. */
  function strokeMetrics(cmds, res) {
    res = res || 24;
    const polys = glyphPolys(cmds, 10); const bb = bboxOf(polys.flat()); if (!bb) return { strokeMm: 0, gapMm: 0 };
    const frame = { w: Math.ceil((bb[2] - bb[0]) * res) + 4, h: Math.ceil((bb[3] - bb[1]) * res) + 4, res, ox: bb[0] - 2 / res, oy: bb[1] - 2 / res };
    const m = rasterPolys(polys, frame); const { w, h, bits } = m;
    const inkRuns = [], gapRuns = [];
    const scan = (sx, sy, dx, dy) => { let run = 0, v = -1, x = sx, y = sy; const step = Math.hypot(dx, dy); while (x >= 0 && y >= 0 && x < w && y < h) { const b = bits[y * w + x]; if (b === v) run++; else { if (v === 1 && run) inkRuns.push(run * step); if (v === 0 && run && x !== sx + dx * run && b === 1) gapRuns.push(run * step); v = b; run = 1; } x += dx; y += dy; } if (v === 1 && run) inkRuns.push(run * step); };
    for (let y = 0; y < h; y++) scan(0, y, 1, 0);
    for (let x = 0; x < w; x++) scan(x, 0, 0, 1);
    for (let x = -h; x < w; x++) scan(x < 0 ? 0 : x, x < 0 ? -x : 0, 1, 1);
    for (let x = 0; x < w + h; x++) scan(x < w ? x : w - 1, x < w ? 0 : x - w + 1, -1, 1);
    // gaps: background runs that start AND end at ink (the scan above only records a run closed by ink; drop the leading one)
    const q = (arr, p) => { if (!arr.length) return 0; const s = arr.slice().sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(p * (s.length - 1)))]; };
    const strokePx = q(inkRuns, 0.05), gapPx = q(gapRuns.filter(g => g <= Math.max(6, 6 * strokePx)), 0.05);
    return { strokeMm: strokePx / res * MM_PER_PT, gapMm: gapPx / res * MM_PER_PT, samples: { ink: inkRuns.length, gap: gapRuns.length } };
  }

  /**
   * lines[]  the customer's text split into lines; font: an opentype.js Font; mask: the eroded back mask (engraveMask)
   * opts: { minCapMm 1.6, maxHeightFrac 0.4, lineGap 0.18, minStrokeMm 0.15, minGapMm 0.12, tryRotated true, angles, rects 6, semiboldBelowMm 2.2, fonts:{Regular, Semibold} }
   * Returns { ok, size, capMm, weight, small, thin, angle, rect, centre, layout, glyphs (cmds, mask frame), metrics, gain } or { ok:false, reason }.
   */
  function fitText(lines, font, mask, opts) {
    opts = Object.assign({ minCapMm: 1.6, maxHeightFrac: 0.4, lineGap: 0.18, minStrokeMm: 0.15, minGapMm: 0.12, tryRotated: true, rects: 6, semiboldBelowMm: 2.2, rotGain: 0.12 }, opts || {});
    lines = (lines || []).map(s => String(s)).filter(s => s.trim().length);
    if (!lines.length) return { ok: false, reason: "no text" };
    if (!area(mask)) return { ok: false, reason: "no solid area on the back" };
    const angles = opts.tryRotated ? (opts.angles || [0, 15, -15, 30, -30, 45, -45, 60, -60, 75, -75, 90, -90]) : [0];
    const maxH = opts.maxHeightFrac * (mask.hPt || mask.h / mask.res);
    const strokeOk = layout => { if (!(opts.minStrokeMm > 0) && !(opts.minGapMm > 0)) return true; const mt = strokeMetrics(layout.cmds, 24); return (!(opts.minStrokeMm > 0) || mt.strokeMm >= opts.minStrokeMm) && (!(opts.minGapMm > 0) || !mt.gapMm || mt.gapMm >= opts.minGapMm); };
    const fitsIn = (b, r) => b[0] >= r.x0 && b[1] >= r.y0 && b[2] <= r.x1 && b[3] <= r.y1;
    let best = null, best0 = null;
    for (const angle of angles) {
      const mA = angle ? rotateMask(mask, -angle) : mask;
      const rects = largestRectangles(mA, opts.rects);
      for (const r of rects) {
        const centre = [(r.x0 + r.x1) / 2, (r.y0 + r.y1) / 2];
        const passes = (size, withStroke) => { const L = layoutLines(lines, font, size, opts.lineGap, 0, centre); if (!fitsIn(L.bbox, r)) return null; if (!verifyInk(L.cmds, mA).ok) return null; if (withStroke && !strokeOk(L)) return null; return L; };
        let lo = 0.01, hi = Math.min(r.hPt, maxH, r.wPt * 4), size = 0, thin = false;
        if (!(hi > lo)) continue;
        for (let withStroke of [true, false]) {
          let l = lo, h = hi; size = 0;
          while (h - l > 0.002) { const mid = (l + h) / 2; if (passes(mid, withStroke)) { size = mid; l = mid; } else h = mid; }
          if (size) { thin = !withStroke; break; }
        }
        if (!size) continue;
        const cand = { size, angle, rect: r, centreA: centre, thin };
        if (angle === 0) { if (!best0 || size > best0.size + 0.01) best0 = cand; }
        else if (!best || size > best.size + 0.01) best = cand;
      }
    }
    if (!best && !best0) return { ok: false, reason: `The text could not fit inside the usable back area. Try another line break or placement.` };
    // the search on a rotated raster is only a guide: each candidate's size is settled again at its final centre and
    // angle against the UNROTATED eroded mask, so what is returned is the largest size that verifies there and 0.1 pt
    // more does not. The block centre goes back into the unrotated frame by rotating it +angle about the mask centre.
    const settle = cand => { if (!cand) return null; const centre = cand.angle ? ap(rotateAbout(mask.cx, mask.cy, cand.angle), cand.centreA[0], cand.centreA[1]) : cand.centreA; const s = refitAt(lines, font, mask, opts, { centre, angle: cand.angle }); return s.ok ? Object.assign(cand, { centre, size: s.size, layout: s.layout }) : null; };
    const s0 = settle(best0), sR = settle(best);
    let pick = s0;
    if (sR && (!s0 || (sR.size - s0.size) / s0.size >= opts.rotGain)) pick = Object.assign(sR, { gain: s0 ? (sR.size - s0.size) / s0.size : Infinity });
    if (!pick) return { ok: false, reason: "the fitted text did not verify against the eroded mask" };
    const size = pick.size, layout = pick.layout, centre = pick.centre;
    const capMm = size * capPerEm(font) * MM_PER_PT;
    const metrics = strokeMetrics(layout.cmds, 24);
    return { ok: true, size, capMm, weight: capMm < opts.semiboldBelowMm ? "Semibold" : "Regular", small: capMm < opts.minCapMm, thin: !!pick.thin, angle: pick.angle, rect: pick.rect, centre, layout, glyphs: layout.glyphs, cmds: layout.cmds, metrics, gain: pick.gain || 0, lines };
  }
  /* How big should the lettering be, given how much there is to say and how much room there is to say it in?
     The largest size that fits is the ceiling, not the answer: a four letter name at the ceiling fills the charm and
     reads as a logo. The share of that ceiling grows with the length of the longest line, smoothly, from SHORT_FILL at
     SHORT_CHARS or fewer to LONG_FILL at LONG_CHARS or more. Two further rules hold it honest: the cap height never
     passes CAP_OF_CHARM of the charm's smaller side, and it never drops below the legible minimum unless the ceiling
     itself is below it. Everything here is a default a person can override by hand afterwards. */
  /* How big should the lettering be? The largest size that fits is the CEILING, not the answer: a four letter name at
     the ceiling spans the charm and reads as a logo. So the longest line is given a share of the usable face, and that
     share GROWS with the line's own measured width — a long message claims more of the face while every letter in it is
     set smaller. Length is read from the font's real kerned advance, never from a character count: "WILLIAM" is seven
     characters and wider than "illinois", which is eight. Three rules hold it honest — never past a share of the charm's
     smaller side, never above the fitted ceiling, never below the legible minimum unless the ceiling is itself below it. */
  const SIZE_RULE = { ADV_REF: 2.78, SPAN_REF: 0.45, Q: 0.8, CAP_OF_CHARM: 0.3, STEP_PT: 0.05 };
  /** opts: { capPerEm, minCapMm, charmMinMm, charmMaxMm, usableAreaMm2, advanceOf(text) → advance at size 1, sizeRule } */
  function defaultSize(lines, fittedMax, opts) {
    opts = opts || {};
    const R = Object.assign({}, SIZE_RULE, opts.sizeRule || {});
    const cpe = opts.capPerEm; if (!(cpe > 0) || !(fittedMax > 0)) return fittedMax;
    const toMm = sz => sz * cpe * MM_PER_PT, toPt = cap => cap / (cpe * MM_PER_PT);
    const capCeil = toMm(fittedMax);
    const lo = Math.min(opts.minCapMm > 0 ? opts.minCapMm : 1.6, capCeil);
    let adv = 0;                                                      // the longest line, measured in its own cap heights
    if (typeof opts.advanceOf === "function") for (const l of lines || []) { const t = String(l).trim(); if (!t) continue; const w = opts.advanceOf(t) / cpe; if (w > adv) adv = w; }
    // how wide the usable face is: the equivalent-circle diameter of the eroded area, which is exact for a disc
    let span = opts.usableAreaMm2 > 0 ? 2 * Math.sqrt(opts.usableAreaMm2 / Math.PI) : opts.charmMinMm;
    if (opts.charmMaxMm > 0) span = Math.min(span, opts.charmMaxMm);
    if (!(adv > 0) || !(span > 0)) return fittedMax;                  // nothing measured: the ceiling stands
    const share = R.SPAN_REF * Math.pow(adv / R.ADV_REF, R.Q);        // ← the whole rule: cap ∝ adv^(Q−1)
    const want = share * span / adv;
    let hi = capCeil;
    if (opts.charmMinMm > 0) hi = Math.min(hi, R.CAP_OF_CHARM * opts.charmMinMm);
    const cap = Math.min(Math.max(want, lo), Math.max(hi, lo));       // max inside min: legibility beats taste, physics beats both
    if (cap >= capCeil - 1e-9) return fittedMax;                      // the ceiling is already verified and settled
    let size = Math.round(toPt(cap) / R.STEP_PT) * R.STEP_PT;
    if (toMm(size) < lo - 1e-9) size = Math.ceil(toPt(lo) / R.STEP_PT) * R.STEP_PT;   // rounding must not land under the floor
    return Math.min(size, fittedMax);
  }
  /** What the size rule was asked and what it was allowed: the slider runs [lo, ceiling] and the default sits between. */
  function sizeRange(fittedMax, opts) {
    opts = opts || {}; const cpe = opts.capPerEm;
    if (!(cpe > 0)) return null;
    const capCeil = fittedMax * cpe * MM_PER_PT;
    return { loMm: Math.min(opts.minCapMm > 0 ? opts.minCapMm : 1.6, capCeil), capCeilMm: capCeil };
  }
  /** Largest size at a fixed centre and angle (a nudge), optionally capped. */
  function refitAt(lines, font, mask, opts, place) {
    opts = Object.assign({ lineGap: 0.18, maxHeightFrac: 0.4, minCapMm: 1.6, semiboldBelowMm: 2.2 }, opts || {});
    const maxH = opts.maxHeightFrac * (mask.hPt || mask.h / mask.res);
    let lo = 0.01, hi = Math.min(place.maxSize || maxH, maxH), size = 0;
    while (hi - lo > 0.002) { const mid = (lo + hi) / 2; const L = layoutLines(lines, font, mid, opts.lineGap, place.angle || 0, place.centre); if (verifyInk(L.cmds, mask).ok) { size = mid; lo = mid; } else hi = mid; }
    if (!size) return { ok: false, reason: "no room at that position" };
    const layout = layoutLines(lines, font, size, opts.lineGap, place.angle || 0, place.centre);
    const capMm = size * capPerEm(font) * MM_PER_PT;
    return { ok: true, size, capMm, weight: capMm < opts.semiboldBelowMm ? "Semibold" : "Regular", small: capMm < opts.minCapMm, angle: place.angle || 0, centre: place.centre, layout, glyphs: layout.glyphs, cmds: layout.cmds, metrics: strokeMetrics(layout.cmds, 24), lines };
  }
  /** Alternative line splits for "Re-split lines": joined, at the customer's breaks, and between a name and a date. */
  // Word boundaries use measured glyph advances; explicit newlines are hard
  // boundaries in Auto. A chosen count deliberately reflows the whole text.
  function wrapLines(text, count, font) {
    const words = String(text).trim().split(/\s+/u).filter(Boolean);
    count = Math.max(1, Math.min(words.length, Number.isFinite(+count) ? Math.round(+count) : 1));
    if (!words.length) return [];
    const width = t => font.getAdvanceWidth(t, 1, {kerning:true});
    const target = width(words.join(" ")) / count;
    const memo = new Map();
    function solve(start, left) {
      if (left === 1) { const line=words.slice(start).join(" "); return {cost:(width(line)-target)**2,lines:[line]}; }
      const key=start+":"+left;if(memo.has(key))return memo.get(key);
      let best=null;
      for(let end=start+1;end<=words.length-left+1;end++) {
        const line=words.slice(start,end).join(" "), rest=solve(end,left-1);
        const cost=(width(line)-target)**2+rest.cost;
        if(!best||cost<best.cost)best={cost,lines:[line,...rest.lines]};
      }
      memo.set(key,best);return best;
    }
    return solve(0,count).lines;
  }
  function fitMultiline(lines, font, mask, opts, mode='auto') {
    lines=(lines||[]).flatMap(s=>String(s).split(/\r?\n/)).map(s=>s.trim()).filter(Boolean);
    if(mode==='preserve'||(mode==='auto'&&lines.length>1))return fitText(lines,font,mask,opts);
    const text=lines.join(" "), words=text.split(/\s+/).filter(Boolean);
    if(mode!=='auto')return fitText(wrapLines(text,+mode,font),font,mask,opts);
    if(words.length<2)return fitText(lines,font,mask,opts);
    const rects=largestRectangles(mask,6),maxH=(opts?.maxHeightFrac||.4)*(mask.hPt||mask.h/mask.res);
    const variants=Array.from({length:Math.min(6,words.length)},(_,i)=>wrapLines(text,i+1,font)).map(v=>{
      const box=layoutLines(v,font,1,.18,0,[0,0]).bbox;
      return {lines:v,score:Math.max(0,...rects.map(r=>Math.min(r.wPt/(box[2]-box[0]),r.hPt/(box[3]-box[1]),maxH)))};
    }).sort((a,b)=>b.score-a.score||a.lines.length-b.lines.length);
    let best=null;
    for(const v of variants.slice(0,2)) {
      const fit=fitText(v.lines,font,mask,opts);
      if(fit.ok&&(!best||fit.size>best.size*1.03))best=fit;
    }
    return best||fitText(lines,font,mask,opts);
  }
  function splitVariants(lines) {
    const joined = lines.join(" ").replace(/\s+/g, " ").trim();
    const out = [[joined]];
    if (lines.length > 1) out.push(lines.slice());
    const m = /^(.*?\S)\s+(\d{1,2}[.\/-]\d{1,2}[.\/-]\d{2,4})$/.exec(joined); if (m) out.push([m[1], m[2]]);
    const words = joined.split(" "); if (words.length >= 2 && words.length <= 4) out.push([words.slice(0, Math.ceil(words.length / 2)).join(" "), words.slice(Math.ceil(words.length / 2)).join(" ")]);
    const seen = new Set(); return out.filter(v => { const k = v.join("\n"); if (seen.has(k)) return false; seen.add(k); return true; });
  }

  /* ═══ 5 · SVG for server thumbnails ════════════════════════════════════ */
  function svgPathOf(seg) {
    const f = v => +v.toFixed(3); let d = "";
    for (const sub of seg.subpaths || []) for (const s of sub) { if (s[0] === "m") d += `M${f(s[1][0])} ${f(s[1][1])}`; else if (s[0] === "l") d += `L${f(s[1][0])} ${f(s[1][1])}`; else if (s[0] === "c") d += `C${f(s[1][0])} ${f(s[1][1])} ${f(s[2][0])} ${f(s[2][1])} ${f(s[3][0])} ${f(s[3][1])}`; else if (s[0] === "h") d += "Z"; }
    return d;
  }
  /** Glyph commands → SVG path d (y flipped for a y-down viewer when `flipY` is given as the frame height). */
  function svgPathOfCmds(cmds) {
    const f = v => +v.toFixed(3); let d = "";
    for (const k of cmds) { if (k.type === "M") d += `M${f(k.x)} ${f(k.y)}`; else if (k.type === "L") d += `L${f(k.x)} ${f(k.y)}`; else if (k.type === "C") d += `C${f(k.x1)} ${f(k.y1)} ${f(k.x2)} ${f(k.y2)} ${f(k.x)} ${f(k.y)}`; else if (k.type === "Q") d += `Q${f(k.x1)} ${f(k.y1)} ${f(k.x)} ${f(k.y)}`; else d += "Z"; }
    return d;
  }
  /** A charm's silhouette bits (outline + cut lines, even-odd) as the sorter's {bits,w,h,scale} — for server-side indexing without a canvas. */
  function silhouetteBits(charm, scale, opts) {
    scale = scale || 6; opts = opts || {};
    const pad = Math.max((charm.outline.lwPt || 0.5) / 2, ...charm.members.map(m => (m.lwPt || 0) / 2)) + 1;
    const b = charm.bbox; const bx0 = b[0] - pad, by0 = b[1] - pad, bx1 = b[2] + pad, by1 = b[3] + pad;
    const w = Math.max(2, Math.ceil((bx1 - bx0) * scale)), h = Math.max(2, Math.ceil((by1 - by0) * scale));
    const frame = { w, h, res: scale, ox: bx0, oy: by0 };
    const m = rasterPolys(flatten(charm.outline, 12), frame);
    if (opts.holesSolid === false) for (const k of charm.members) if (k !== charm.outline && isCutLine(k)) rasterPolys(flatten(k, 12), frame, m, 0);
    // pixel rows are stored y-up here; the sorter's canvas bits are y-down — flip rows so hashes and nesting agree
    const bits = new Uint8Array(w * h); for (let y = 0; y < h; y++) bits.set(m.bits.subarray((h - 1 - y) * w, (h - y) * w), y * w);
    let n = 0; for (let i = 0; i < bits.length; i++) n += bits[i];
    return { bits, w, h, scale, areaPt2: n / (scale * scale), bboxOuter: [bx0, by0, bx1, by1] };
  }

  return { MM_PER_PT, PT_PER_MM, mul, ap, mirrorX, rotateAbout, translate, transformSeg, flatten, polyCentroid, pointInPolys, distToPolys, interiorPoint,
    makeFrame, emptyMask, cloneMask, rasterPolys, raster, area, flipX, diffFraction, at, distanceTransform, erode, subtract, rotateMask, largestRectangles,
    isCutLine, BackViewError, upAngleOf, backView, engraveMask,
    glyphCoverage, capPerEm, lineGlyphs, layoutLines, glyphPolys, rasterGlyphs, verifyInk, strokeMetrics, fitText, refitAt, defaultSize, sizeRange, SIZE_RULE, splitVariants, wrapLines, fitMultiline,
    svgPathOf, svgPathOfCmds, silhouetteBits };
});
