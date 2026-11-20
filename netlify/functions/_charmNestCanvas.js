/*  netlify/functions/_charmNestCanvas.js
 *  The page's canvas, for node (cloud plan v2, build step 3). charm-nest-pdf.js draws a charm's silhouette (buildSilhouettes: the outline stroked and filled,
 *  every member drawn on top, the pixels read back and flooded from the border) and its thumbnail on a canvas; on a page that is OffscreenCanvas. Node has
 *  none, so a piece could not be made there. This is one: a 2D context that RECORDS what is drawn (paths in device pixels, with the paint, the line width,
 *  joins, caps and the fill rule), and draws the record only when its pixels are asked for (getImageData, toDataURL).
 *
 *  Two ways to draw the record, one record:
 *    "resvg"  the record written as an SVG document and rendered by @resvg/resvg-js (a dependency of the site, netlify.toml external_node_modules), the
 *             way the plan chose it: tiny-skia is a port of Skia's rasteriser, which the page's canvas uses;
 *    "js"     the same paths filled here, in plain JavaScript: the exact geometry (curves flattened finer than a twentieth of a pixel; a stroke is the union
 *             of its segments, joins and caps as polygons, filled with the non-zero rule), covered by area (16 rows a pixel, each crossing exact along its
 *             row), so a host with no resvg (a test machine) draws the same pieces.
 *  install(global, { backend }) puts it in place as OffscreenCanvas, only when asked (netlify/functions/_charmNestPdf.js withCanvas()): nothing that runs
 *  today loads it. backend: "resvg" | "js" | "auto" (the default: resvg when it loads, else js); CHARM_NEST_CANVAS in the environment picks one too.
 *  tests/charm-nest/cloud-silhouette-parity.cjs holds both to a golden recorded once from the page's own canvas (Chrome): the silhouette bits, its hash and
 *  its area. Measured on 20 Nov 2026 (Chrome 158, 224 charms): every pixel "js" draws differently lies on the edge of the page's silhouette and every area is
 *  within 1 %, but Chrome flattens curves and strokes more coarsely (its silhouettes are a little thinner at curved edges), and the 24 x 24 majority grid
 *  the hash is made of flips on single edge pixels: the hash is the page's for about half the charms only. "resvg" was not measured (not installed here).
 *  What it does not draw: text (fillText draws nothing; the silhouettes and thumbnails draw a text member as its box) and images (drawImage).            */
"use strict";
const zlib = require("zlib");

/* ═══ 1 · paint ═══ */
const NAMED = { black: [0, 0, 0, 1], white: [255, 255, 255, 1], transparent: [0, 0, 0, 0], red: [255, 0, 0, 1], green: [0, 128, 0, 1], blue: [0, 0, 255, 1], grey: [128, 128, 128, 1], gray: [128, 128, 128, 1] };
/** A CSS colour as [r, g, b, a] (0-255, 0-1), or null when it is not one (the canvas then keeps the paint it had). */
function parseColor(v) {
  const s = String(v == null ? "" : v).trim().toLowerCase();
  if (NAMED[s]) return NAMED[s].slice();
  let m = /^#([0-9a-f]{3,8})$/.exec(s);
  if (m) {
    const h = m[1], d = h.length === 3 || h.length === 4 ? h.split("").map(c => parseInt(c + c, 16)) : h.length === 6 || h.length === 8 ? h.match(/../g).map(x => parseInt(x, 16)) : null;
    if (!d) return null; return [d[0], d[1], d[2], d.length === 4 ? d[3] / 255 : 1];
  }
  m = /^rgba?\(([^)]*)\)$/.exec(s);
  if (m) {
    const p = m[1].split(/[\s,/]+/).filter(Boolean); if (p.length < 3) return null;
    const ch = x => (/%$/.test(x) ? parseFloat(x) * 2.55 : parseFloat(x)), al = x => (x == null ? 1 : /%$/.test(x) ? parseFloat(x) / 100 : parseFloat(x));
    const out = [ch(p[0]), ch(p[1]), ch(p[2]), al(p[3])]; if (out.some(x => !Number.isFinite(x))) return null;
    return [Math.max(0, Math.min(255, Math.round(out[0]))), Math.max(0, Math.min(255, Math.round(out[1]))), Math.max(0, Math.min(255, Math.round(out[2]))), Math.max(0, Math.min(1, out[3]))];
  }
  return null;
}

/* ═══ 2 · the context: a record of what is drawn ═══ */
const IDENTITY = [1, 0, 0, 1, 0, 0];
const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
const apply = (m, x, y) => [m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5]];
class Context2D {
  constructor(canvas) {
    this.canvas = canvas; this.ops = []; this.path = []; this.cur = null; this.stack = [];
    this.st = { fill: [0, 0, 0, 1], fillStyle: "#000000", stroke: [0, 0, 0, 1], strokeStyle: "#000000", lineWidth: 1, lineJoin: "miter", lineCap: "butt", miterLimit: 10, globalAlpha: 1, m: IDENTITY.slice(), font: "10px sans-serif", textAlign: "start", textBaseline: "alphabetic" };
    this._px = null; this._done = 0;
  }
  // ── state ──
  get fillStyle() { return this.st.fillStyle; } set fillStyle(v) { const c = parseColor(v); if (c) { this.st.fill = c; this.st.fillStyle = String(v); } }
  get strokeStyle() { return this.st.strokeStyle; } set strokeStyle(v) { const c = parseColor(v); if (c) { this.st.stroke = c; this.st.strokeStyle = String(v); } }
  get lineWidth() { return this.st.lineWidth; } set lineWidth(v) { v = +v; if (Number.isFinite(v) && v > 0) this.st.lineWidth = v; }
  get lineJoin() { return this.st.lineJoin; } set lineJoin(v) { if (v === "round" || v === "bevel" || v === "miter") this.st.lineJoin = v; }
  get lineCap() { return this.st.lineCap; } set lineCap(v) { if (v === "round" || v === "square" || v === "butt") this.st.lineCap = v; }
  get miterLimit() { return this.st.miterLimit; } set miterLimit(v) { v = +v; if (Number.isFinite(v) && v > 0) this.st.miterLimit = v; }
  get globalAlpha() { return this.st.globalAlpha; } set globalAlpha(v) { v = +v; if (Number.isFinite(v) && v >= 0 && v <= 1) this.st.globalAlpha = v; }
  get font() { return this.st.font; } set font(v) { this.st.font = String(v); }
  get textAlign() { return this.st.textAlign; } set textAlign(v) { this.st.textAlign = String(v); }
  get textBaseline() { return this.st.textBaseline; } set textBaseline(v) { this.st.textBaseline = String(v); }
  save() { this.stack.push(Object.assign({}, this.st, { fill: this.st.fill.slice(), stroke: this.st.stroke.slice(), m: this.st.m.slice() })); }
  restore() { const s = this.stack.pop(); if (s) this.st = s; }
  setTransform(a, b, c, d, e, f) { this.st.m = typeof a === "object" && a ? [a.a, a.b, a.c, a.d, a.e, a.f] : [a, b, c, d, e, f].map(Number); }
  resetTransform() { this.st.m = IDENTITY.slice(); }
  transform(a, b, c, d, e, f) { this.st.m = mul(this.st.m, [a, b, c, d, e, f].map(Number)); }
  translate(x, y) { this.transform(1, 0, 0, 1, x, y); }
  scale(x, y) { this.transform(x, 0, 0, y == null ? x : y, 0, 0); }
  rotate(a) { const c = Math.cos(a), s = Math.sin(a); this.transform(c, s, -s, c, 0, 0); }
  // ── the path, in device pixels (the transform at the time each point is added, as a canvas does) ──
  beginPath() { this.path = []; this.cur = null; }
  _sub() { if (!this.cur) { this.cur = { pts: [], closed: false, drew: false }; this.path.push(this.cur); } return this.cur; }
  moveTo(x, y) { this.cur = { pts: [apply(this.st.m, +x, +y)], closed: false, drew: false }; this.path.push(this.cur); }
  lineTo(x, y) { const p = apply(this.st.m, +x, +y); if (!this.cur || !this.cur.pts.length) { this.moveTo(x, y); return; } if (this.cur.closed) this._reopen(); this.cur.pts.push(p); this.cur.drew = true; }
  _reopen() { const s = this.cur.pts[0]; this.cur = { pts: [s.slice()], closed: false, drew: false }; this.path.push(this.cur); }
  bezierCurveTo(x1, y1, x2, y2, x, y) {
    if (!this.cur || !this.cur.pts.length) this.moveTo(x1, y1); else if (this.cur.closed) this._reopen();
    const p0 = this.cur.pts[this.cur.pts.length - 1], p1 = apply(this.st.m, +x1, +y1), p2 = apply(this.st.m, +x2, +y2), p3 = apply(this.st.m, +x, +y);
    flattenCubic(p0, p1, p2, p3, this.cur.pts, 0); this.cur.drew = true;
  }
  quadraticCurveTo(x1, y1, x, y) {
    if (!this.cur || !this.cur.pts.length) this.moveTo(x1, y1); else if (this.cur.closed) this._reopen();
    const p0 = this.cur.pts[this.cur.pts.length - 1], q = apply(this.st.m, +x1, +y1), p3 = apply(this.st.m, +x, +y);
    flattenCubic(p0, [p0[0] + 2 / 3 * (q[0] - p0[0]), p0[1] + 2 / 3 * (q[1] - p0[1])], [p3[0] + 2 / 3 * (q[0] - p3[0]), p3[1] + 2 / 3 * (q[1] - p3[1])], p3, this.cur.pts, 0); this.cur.drew = true;
  }
  closePath() { if (this.cur && this.cur.pts.length) { this.cur.closed = true; this.cur.drew = true; } }
  rect(x, y, w, h) { this.moveTo(x, y); this.lineTo(x + w, y); this.lineTo(x + w, y + h); this.lineTo(x, y + h); this.closePath(); }
  arc(cx, cy, r, a0, a1, ccw) {
    r = Math.abs(+r); let sweep = a1 - a0;
    if (!ccw && sweep < 0) sweep = sweep % (2 * Math.PI) + 2 * Math.PI; if (ccw && sweep > 0) sweep = sweep % (2 * Math.PI) - 2 * Math.PI;
    if (Math.abs(a1 - a0) >= 2 * Math.PI) sweep = ccw ? -2 * Math.PI : 2 * Math.PI;
    const k = this.st.m, scale = Math.sqrt(Math.abs(k[0] * k[3] - k[1] * k[2])) || 1, n = Math.max(4, Math.ceil(Math.abs(sweep) / arcStep(r * scale)));
    for (let i = 0; i <= n; i++) { const a = a0 + sweep * i / n, x = cx + r * Math.cos(a), y = cy + r * Math.sin(a); if (i === 0 && this.cur && this.cur.pts.length) this.lineTo(x, y); else if (i === 0) this.moveTo(x, y); else this.lineTo(x, y); }
  }
  // ── painting: each is recorded with the paint and the path as they are now ──
  _paths() { return this.path.filter(s => s.pts.length).map(s => ({ pts: s.pts.map(p => p.slice()), closed: s.closed, drew: s.drew })); }
  fill(a, b) { const rule = (typeof a === "string" ? a : b) === "evenodd" ? "evenodd" : "nonzero"; this.ops.push({ op: "fill", subs: this._paths(), rule, color: this.st.fill.slice(), alpha: this.st.globalAlpha }); }
  stroke() {
    const m = this.st.m, scale = Math.sqrt(Math.abs(m[0] * m[3] - m[1] * m[2])) || 1;
    this.ops.push({ op: "stroke", subs: this._paths(), color: this.st.stroke.slice(), alpha: this.st.globalAlpha, width: this.st.lineWidth * scale, join: this.st.lineJoin, cap: this.st.lineCap, miterLimit: this.st.miterLimit });
  }
  _box(x, y, w, h) { const m = this.st.m; return [apply(m, x, y), apply(m, x + w, y), apply(m, x + w, y + h), apply(m, x, y + h)]; }
  fillRect(x, y, w, h) { if (!(w && h)) return; this.ops.push({ op: "fill", subs: [{ pts: this._box(+x, +y, +w, +h), closed: true, drew: true }], rule: "nonzero", color: this.st.fill.slice(), alpha: this.st.globalAlpha }); }
  strokeRect(x, y, w, h) { const keep = this.path, cur = this.cur; this.beginPath(); this.rect(x, y, w, h); this.stroke(); this.path = keep; this.cur = cur; }
  clearRect(x, y, w, h) {
    const box = this._box(+x, +y, +w, +h), W = this.canvas.width, H = this.canvas.height;
    const xs = box.map(p => p[0]), ys = box.map(p => p[1]), ident = this.st.m[1] === 0 && this.st.m[2] === 0;
    if (ident && Math.min(...xs) <= 0 && Math.min(...ys) <= 0 && Math.max(...xs) >= W && Math.max(...ys) >= H) { this.ops = []; this._px = null; this._done = 0; return; }   // (the whole canvas: nothing drawn before is left)
    this.ops.push({ op: "clear", subs: [{ pts: box, closed: true, drew: true }] });
  }
  fillText() { /* not drawn: see the head of this file */ }
  strokeText() { /* not drawn */ }
  measureText(t) { const px = parseFloat(/(\d+(?:\.\d+)?)px/.exec(this.st.font)?.[1]) || 10; return { width: String(t == null ? "" : t).length * px * 0.55, actualBoundingBoxAscent: px * 0.75, actualBoundingBoxDescent: px * 0.25 }; }
  drawImage() { /* not drawn */ }
  createLinearGradient() { return { addColorStop() {} }; }
  createRadialGradient() { return { addColorStop() {} }; }
  setLineDash() {} getLineDash() { return []; }
  // ── the pixels ──
  getImageData(x, y, w, h) {
    const W = this.canvas.width, H = this.canvas.height, px = render(this);
    x = Math.floor(+x || 0); y = Math.floor(+y || 0); w = Math.floor(+w || 0); h = Math.floor(+h || 0);
    const data = new Uint8ClampedArray(w * h * 4);
    for (let r = 0; r < h; r++) { const sy = y + r; if (sy < 0 || sy >= H) continue; for (let c = 0; c < w; c++) { const sx = x + c; if (sx < 0 || sx >= W) continue; const i = (sy * W + sx) * 4, o = (r * w + c) * 4; data[o] = px[i]; data[o + 1] = px[i + 1]; data[o + 2] = px[i + 2]; data[o + 3] = px[i + 3]; } }
    return { data, width: w, height: h, colorSpace: "srgb" };
  }
}
class NodeCanvas {
  constructor(w, h) { this.width = Math.max(0, Math.floor(+w || 0)); this.height = Math.max(0, Math.floor(+h || 0)); this._ctx = null; }
  getContext(type) { if (type !== "2d") return null; return this._ctx || (this._ctx = new Context2D(this)); }
  /** The drawing as a PNG data address (the page's canvas.toDataURL("image/png")). */
  toDataURL() { return "data:image/png;base64," + pngOf(this).toString("base64"); }
  toBuffer() { return pngOf(this); }
}

/* ═══ 3 · curves and arcs as lines, finer than a twentieth of a pixel ═══ */
const TOL = 0.05;
function flattenCubic(p0, p1, p2, p3, out, depth) {
  const dx = p3[0] - p0[0], dy = p3[1] - p0[1], len = Math.hypot(dx, dy);
  const d1 = len > 1e-9 ? Math.abs((p1[0] - p3[0]) * dy - (p1[1] - p3[1]) * dx) / len : Math.hypot(p1[0] - p0[0], p1[1] - p0[1]);
  const d2 = len > 1e-9 ? Math.abs((p2[0] - p3[0]) * dy - (p2[1] - p3[1]) * dx) / len : Math.hypot(p2[0] - p0[0], p2[1] - p0[1]);
  if (depth >= 18 || d1 + d2 <= TOL) { out.push(p3.slice()); return; }
  const m01 = mid(p0, p1), m12 = mid(p1, p2), m23 = mid(p2, p3), a = mid(m01, m12), b = mid(m12, m23), c = mid(a, b);
  flattenCubic(p0, m01, a, c, out, depth + 1); flattenCubic(c, b, m23, p3, out, depth + 1);
}
const mid = (a, b) => [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2];
const arcStep = r => (r > TOL ? 2 * Math.acos(Math.max(-1, 1 - TOL / r)) : Math.PI / 2);

/* ═══ 4 · a stroke as polygons: each segment a quad, each join and cap its own piece (all turned the same way, so the non-zero rule unites them) ═══ */
function ccw(poly) { let a = 0; for (let i = 0, n = poly.length; i < n; i++) { const p = poly[i], q = poly[(i + 1) % n]; a += p[0] * q[1] - q[0] * p[1]; } return a < 0 ? poly.reverse() : poly; }
function fan(c, r, a0, a1, withCenter) {   // an arc from a0 to a1 (radians, a1 > a0), as points; with its centre: a pie slice
  const n = Math.max(1, Math.ceil((a1 - a0) / arcStep(r))), out = withCenter ? [c.slice()] : [];
  for (let i = 0; i <= n; i++) { const a = a0 + (a1 - a0) * i / n; out.push([c[0] + r * Math.cos(a), c[1] + r * Math.sin(a)]); }
  return out;
}
function strokePolys(op) {
  const hw = op.width / 2, polys = [];
  for (const s of op.subs) {
    const pts = []; for (const p of s.pts) { const q = pts[pts.length - 1]; if (!q || Math.hypot(p[0] - q[0], p[1] - q[1]) > 1e-9) pts.push(p); }
    let closed = s.closed; if (closed && pts.length > 1 && Math.hypot(pts[0][0] - pts[pts.length - 1][0], pts[0][1] - pts[pts.length - 1][1]) <= 1e-9) pts.pop();
    if (pts.length === 1) {   // a point: a cap's worth of ink for a round or square cap, nothing for butt (Skia's rule for a zero-length line)
      if (!s.drew) continue;
      if (op.cap === "round") polys.push(ccw(fan(pts[0], hw, 0, 2 * Math.PI, false)));
      else if (op.cap === "square") { const [x, y] = pts[0]; polys.push(ccw([[x - hw, y - hw], [x + hw, y - hw], [x + hw, y + hw], [x - hw, y + hw]])); }
      continue;
    }
    if (pts.length === 2) closed = false;
    const n = pts.length, segs = closed ? n : n - 1;
    const dir = i => { const a = pts[i], b = pts[(i + 1) % n], l = Math.hypot(b[0] - a[0], b[1] - a[1]); return [(b[0] - a[0]) / l, (b[1] - a[1]) / l]; };
    for (let i = 0; i < segs; i++) {
      const a = pts[i], b = pts[(i + 1) % n], d = dir(i), nx = -d[1] * hw, ny = d[0] * hw;
      polys.push(ccw([[a[0] + nx, a[1] + ny], [b[0] + nx, b[1] + ny], [b[0] - nx, b[1] - ny], [a[0] - nx, a[1] - ny]]));
    }
    // joins: at every vertex between two segments
    const joinAt = (i, din, dout) => {
      const p = pts[i], cross = din[0] * dout[1] - din[1] * dout[0], dot = din[0] * dout[0] + din[1] * dout[1];
      if (Math.abs(cross) < 1e-12 && dot > 0) return;   // straight on
      if (Math.abs(cross) < 1e-12) {   // straight back: a round join is the half disc ahead of the turn; a miter has no tip (a bevel of nothing)
        if (op.join === "round") { const t = Math.atan2(din[1], din[0]); polys.push(ccw(fan(p, hw, t - Math.PI / 2, t + Math.PI / 2, true))); }
        return;
      }
      // the outer side is where the turn opens (in pixels, y down: a turn with cross > 0 turns clockwise, its outer side is the left of the way it goes)
      const s0 = cross > 0 ? [din[1] * hw, -din[0] * hw] : [-din[1] * hw, din[0] * hw], s1 = cross > 0 ? [dout[1] * hw, -dout[0] * hw] : [-dout[1] * hw, dout[0] * hw];
      const o0 = [p[0] + s0[0], p[1] + s0[1]], o1 = [p[0] + s1[0], p[1] + s1[1]];
      // (the arc turns as the path turns: from s0 to s1 with the angle growing when cross > 0, shrinking when cross < 0; always the short way, under half a turn)
      if (op.join === "round") { let a0 = Math.atan2(s0[1], s0[0]), a1 = Math.atan2(s1[1], s1[0]); if (cross < 0) [a0, a1] = [a1, a0]; while (a1 < a0) a1 += 2 * Math.PI; polys.push(ccw(fan(p, hw, a0, a1, true))); return; }
      const theta = Math.acos(Math.max(-1, Math.min(1, dot)));   // the turn
      const miterLen = 1 / Math.max(1e-12, Math.cos(theta / 2));   // (miter length / line width)
      if (op.join === "miter" && miterLen <= op.miterLimit) { const bis = [s0[0] + s1[0], s0[1] + s1[1]], bl = Math.hypot(bis[0], bis[1]) || 1, k = hw * miterLen; polys.push(ccw([p.slice(), o0, [p[0] + bis[0] / bl * k, p[1] + bis[1] / bl * k], o1])); return; }
      polys.push(ccw([p.slice(), o0, o1]));   // bevel (and a miter past its limit)
    };
    for (let i = closed ? 0 : 1; i < (closed ? n : n - 1); i++) joinAt(i, dir((i - 1 + n) % n), dir(i));
    if (!closed) {   // caps
      const d0 = dir(0), d1 = dir(n - 2), a = pts[0], b = pts[n - 1];
      if (op.cap === "round") { const t0 = Math.atan2(d0[1], d0[0]); polys.push(ccw(fan(a, hw, t0 + Math.PI / 2, t0 + 3 * Math.PI / 2, true))); const t1 = Math.atan2(d1[1], d1[0]); polys.push(ccw(fan(b, hw, t1 - Math.PI / 2, t1 + Math.PI / 2, true))); }
      else if (op.cap === "square") {
        const sq = (p, d, out) => { const nx = -d[1] * hw, ny = d[0] * hw, ex = d[0] * hw * out, ey = d[1] * hw * out; return ccw([[p[0] + nx, p[1] + ny], [p[0] + nx + ex, p[1] + ny + ey], [p[0] - nx + ex, p[1] - ny + ey], [p[0] - nx, p[1] - ny]]); };
        polys.push(sq(a, d0, -1)); polys.push(sq(b, d1, 1));
      }
    }
  }
  return polys;
}

/* ═══ 5 · coverage: SUB rows a pixel, at their centres, each crossing exact along its row (Chrome's canvas samples 4 rows; 4 here closed a hairline gap of
   two monogram charms that Chrome leaves open, 16 keeps every difference on the edge: tests/charm-nest/cloud-silhouette-parity.cjs) ═══ */
const SUB = 16;
function coverage(polys, rule, W, H) {
  const edges = [];
  let y0 = Infinity, y1 = -Infinity;
  for (const poly of polys) for (let i = 0, n = poly.length; i < n; i++) {
    const a = poly[i], b = poly[(i + 1) % n]; if (a[1] === b[1] || !Number.isFinite(a[0] + a[1] + b[0] + b[1])) continue;
    const up = a[1] < b[1], lo = up ? a : b, hi = up ? b : a;
    edges.push({ ymin: lo[1], ymax: hi[1], x: lo[0], k: (hi[0] - lo[0]) / (hi[1] - lo[1]), w: up ? 1 : -1 });
    if (lo[1] < y0) y0 = lo[1]; if (hi[1] > y1) y1 = hi[1];
  }
  const cov = new Float32Array(W * H); if (!edges.length) return { cov, r0: 0, r1: -1 };
  edges.sort((p, q) => p.ymin - q.ymin);
  const r0 = Math.max(0, Math.floor(y0)), r1 = Math.min(H - 1, Math.ceil(y1));
  const row = new Float64Array(W + 2), full = new Float64Array(W + 2), xs = [];
  let next = 0, active = [];
  const inside = rule === "evenodd" ? w => (w & 1) === 1 : w => w !== 0, f = 1 / SUB;
  const span = (xa, xb) => {
    if (xa < 0) xa = 0; if (xb > W) xb = W; if (!(xb > xa)) return;
    const ia = Math.floor(xa), ib = Math.floor(xb);
    if (ia === ib) { row[ia] += (xb - xa) * f; return; }
    row[ia] += (ia + 1 - xa) * f; if (ib > ia + 1) { full[ia + 1] += f; full[ib] -= f; } if (ib < W) row[ib] += (xb - ib) * f;
  };
  for (let r = r0; r <= r1; r++) {
    row.fill(0); full.fill(0);
    for (let s = 0; s < SUB; s++) {
      const y = r + (s + 0.5) * f;
      while (next < edges.length && edges[next].ymin <= y) active.push(edges[next++]);
      active = active.filter(e => e.ymax > y);
      xs.length = 0; for (const e of active) if (e.ymin <= y) xs.push([e.x + (y - e.ymin) * e.k, e.w]);
      if (xs.length < 2) continue;
      xs.sort((p, q) => p[0] - q[0]);
      let w = 0, start = 0;
      for (const [x, d] of xs) { const was = inside(w); w += d; const now = inside(w); if (!was && now) start = x; else if (was && !now) span(start, x); }
    }
    let run = 0; const o = r * W;
    for (let i = 0; i < W; i++) { run += full[i]; const c = row[i] + run; if (c > 0) cov[o + i] = c > 1 ? 1 : c; }
  }
  return { cov, r0, r1 };
}

/* ═══ 6 · drawing the record ═══ */
function wantBackend() {
  const env = String(process.env.CHARM_NEST_CANVAS || "").toLowerCase();
  return env === "js" || env === "resvg" ? env : install.backend || "auto";
}
let resvgMod;   // undefined: not tried; null: not here
function resvg() { if (resvgMod === undefined) { try { resvgMod = require("@resvg/resvg-js"); } catch (_) { resvgMod = null; } } return resvgMod; }
/** Which way the record is drawn here: "resvg" or "js". */
function backend() { const want = wantBackend(); if (want === "js") return "js"; if (resvg()) return "resvg"; if (want === "resvg") throw new Error("@resvg/resvg-js is not installed here (CHARM_NEST_CANVAS=resvg)"); return "js"; }
/** The canvas's pixels (straight RGBA, 8 bits, as getImageData gives them), drawn from the record. */
function render(ctx) {
  const W = ctx.canvas.width, H = ctx.canvas.height;
  if (backend() === "resvg") { const { pixels } = decodePng(resvgPng(ctx)); return pixels; }
  // js: the record is drawn once; what is drawn after is added on top (premultiplied floats, source-over)
  if (!ctx._px || ctx._px.length !== W * H * 4) { ctx._px = new Float32Array(W * H * 4); ctx._done = 0; }
  const P = ctx._px;
  for (; ctx._done < ctx.ops.length; ctx._done++) {
    const op = ctx.ops[ctx._done];
    const polys = op.op === "stroke" ? strokePolys(op) : op.subs.filter(s => s.pts.length > 2).map(s => s.pts);
    if (!polys.length) continue;
    const { cov, r0, r1 } = coverage(polys, op.op === "stroke" ? "nonzero" : op.rule || "nonzero", W, H);
    if (op.op === "clear") { for (let r = r0; r <= r1; r++) for (let i = r * W, e = i + W; i < e; i++) { const c = cov[i]; if (c > 0) { const k = 1 - c; P[i * 4] *= k; P[i * 4 + 1] *= k; P[i * 4 + 2] *= k; P[i * 4 + 3] *= k; } } continue; }
    const [cr, cg, cb, ca] = op.color, a0 = ca * (op.alpha == null ? 1 : op.alpha), R = cr / 255, G = cg / 255, Bl = cb / 255;
    for (let r = r0; r <= r1; r++) for (let i = r * W, e = i + W; i < e; i++) {
      const c = cov[i]; if (!(c > 0)) continue;
      const sa = c * a0, k = 1 - sa, j = i * 4;
      P[j] = R * sa + P[j] * k; P[j + 1] = G * sa + P[j + 1] * k; P[j + 2] = Bl * sa + P[j + 2] * k; P[j + 3] = sa + P[j + 3] * k;
    }
  }
  const out = new Uint8ClampedArray(W * H * 4);
  for (let j = 0; j < out.length; j += 4) { const a = P[j + 3]; if (!(a > 0)) continue; out[j + 3] = Math.round(a * 255); out[j] = Math.round(P[j] / a * 255); out[j + 1] = Math.round(P[j + 1] / a * 255); out[j + 2] = Math.round(P[j + 2] / a * 255); }
  return out;
}

/* ═══ 7 · the record as SVG, for resvg ═══ */
const f4 = v => (Math.round(v * 10000) / 10000).toString();
const dOf = subs => subs.map(s => (s.pts.length ? "M" + s.pts.map(p => f4(p[0]) + " " + f4(p[1])).join("L") + (s.closed ? "Z" : "") : "")).join("");
const paint = c => `rgb(${c[0]},${c[1]},${c[2]})`;
/** The record as one SVG document in device pixels (shape-rendering geometricPrecision: anti-aliased, as a canvas is). */
function svgOf(ctx) {
  const W = ctx.canvas.width, H = ctx.canvas.height;
  let body = "", masks = "", n = 0;
  for (const op of ctx.ops) {
    if (op.op === "clear") { const id = "c" + n++; masks += `<mask id="${id}" maskUnits="userSpaceOnUse" x="0" y="0" width="${W}" height="${H}"><rect x="0" y="0" width="${W}" height="${H}" fill="#fff"/><path d="${dOf(op.subs)}" fill="#000"/></mask>`; body = `<g mask="url(#${id})">${body}</g>`; continue; }
    const subs = op.op === "fill" ? op.subs.filter(s => s.pts.length > 1) : op.subs.filter(s => s.drew || s.pts.length > 1);
    if (!subs.length) continue;
    const a = op.color[3] * (op.alpha == null ? 1 : op.alpha);
    if (op.op === "fill") body += `<path d="${dOf(subs)}" fill="${paint(op.color)}"${a < 1 ? ` fill-opacity="${f4(a)}"` : ""} fill-rule="${op.rule}"/>`;
    else body += `<path d="${subs.map(s => (s.pts.length === 1 ? `M${f4(s.pts[0][0])} ${f4(s.pts[0][1])}L${f4(s.pts[0][0])} ${f4(s.pts[0][1])}` : dOf([s]))).join("")}" fill="none" stroke="${paint(op.color)}"${a < 1 ? ` stroke-opacity="${f4(a)}"` : ""} stroke-width="${f4(op.width)}" stroke-linejoin="${op.join}" stroke-linecap="${op.cap}" stroke-miterlimit="${f4(op.miterLimit)}"/>`;
  }
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" shape-rendering="geometricPrecision"><defs>${masks}</defs>${body}</svg>`;
}
function resvgPng(ctx) {
  const { Resvg } = resvg();
  const r = new Resvg(svgOf(ctx), { fitTo: { mode: "original" }, shapeRendering: 2, font: { loadSystemFonts: false } });
  return Buffer.from(r.render().asPng());
}

/* ═══ 8 · PNG, written and read (8-bit RGBA, no interlace: what this file and resvg write) ═══ */
const CRC = (() => { const t = new Int32Array(256); for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; t[n] = c; } return t; })();
const crc32 = buf => { let c = -1; for (let i = 0; i < buf.length; i++) c = CRC[(c ^ buf[i]) & 0xff] ^ (c >>> 8); return (c ^ -1) >>> 0; };
function chunk(type, data) { const len = Buffer.alloc(4); len.writeUInt32BE(data.length); const td = Buffer.concat([Buffer.from(type, "ascii"), data]), crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(td)); return Buffer.concat([len, td, crc]); }
function encodePng(W, H, rgba) {
  const raw = Buffer.alloc((W * 4 + 1) * H);
  for (let y = 0; y < H; y++) { raw[y * (W * 4 + 1)] = 0; Buffer.from(rgba.buffer, rgba.byteOffset + y * W * 4, W * 4).copy(raw, y * (W * 4 + 1) + 1); }
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(W, 0); ihdr.writeUInt32BE(H, 4); ihdr[8] = 8; ihdr[9] = 6; ihdr[10] = 0; ihdr[11] = 0; ihdr[12] = 0;
  return Buffer.concat([Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), chunk("IHDR", ihdr), chunk("IDAT", zlib.deflateSync(raw)), chunk("IEND", Buffer.alloc(0))]);
}
function decodePng(buf) {
  let p = 8, W = 0, H = 0, depth = 0, type = 0; const idat = [];
  while (p < buf.length) { const len = buf.readUInt32BE(p), t = buf.toString("ascii", p + 4, p + 8), d = buf.subarray(p + 8, p + 8 + len); if (t === "IHDR") { W = d.readUInt32BE(0); H = d.readUInt32BE(4); depth = d[8]; type = d[9]; if (d[12]) throw new Error("interlaced PNG"); } else if (t === "IDAT") idat.push(d); else if (t === "IEND") break; p += 12 + len; }
  if (depth !== 8 || (type !== 6 && type !== 2)) throw new Error(`PNG of depth ${depth} type ${type}`);
  const bpp = type === 6 ? 4 : 3, raw = zlib.inflateSync(Buffer.concat(idat)), stride = W * bpp, out = new Uint8ClampedArray(W * H * 4), line = Buffer.alloc(stride), prev = Buffer.alloc(stride);
  for (let y = 0; y < H; y++) {
    const ft = raw[y * (stride + 1)], src = raw.subarray(y * (stride + 1) + 1, (y + 1) * (stride + 1));
    for (let i = 0; i < stride; i++) {
      const a = i >= bpp ? line[i - bpp] : 0, b = prev[i], c = i >= bpp ? prev[i - bpp] : 0;
      let v = src[i];
      if (ft === 1) v += a; else if (ft === 2) v += b; else if (ft === 3) v += (a + b) >> 1; else if (ft === 4) { const pp = a + b - c, pa = Math.abs(pp - a), pb = Math.abs(pp - b), pc = Math.abs(pp - c); v += pa <= pb && pa <= pc ? a : pb <= pc ? b : c; }
      line[i] = v & 0xff;
    }
    for (let x = 0; x < W; x++) { const o = (y * W + x) * 4; out[o] = line[x * bpp]; out[o + 1] = line[x * bpp + 1]; out[o + 2] = line[x * bpp + 2]; out[o + 3] = bpp === 4 ? line[x * bpp + 3] : 255; }
    line.copy(prev);
  }
  return { width: W, height: H, pixels: out };
}
function pngOf(canvas) {
  const ctx = canvas.getContext("2d");
  if (backend() === "resvg") return resvgPng(ctx);
  return encodePng(canvas.width, canvas.height, render(ctx));
}

/* ═══ 9 · in place ═══ */
/** Puts the node canvas in place as OffscreenCanvas on `g` (globalThis), when it has none; o.backend picks how the record is drawn. → the class in place. */
function install(g, o) {
  g = g || globalThis;
  if (o && o.backend) install.backend = o.backend;
  if (typeof g.OffscreenCanvas === "undefined") g.OffscreenCanvas = NodeCanvas;
  return g.OffscreenCanvas;
}
install.backend = "auto";

module.exports = { NodeCanvas, Context2D, install, backend, svgOf, parseColor, encodePng, decodePng, coverage, strokePolys };
