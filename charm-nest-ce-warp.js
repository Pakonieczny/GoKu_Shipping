/* charm-nest-ce-warp.js — BLENDS AND WARPS in the Charm Editor (Paul, 28 Oct 2026: "designing custom charms, which requires blinds and deforming lines basically a
 * (Designer) of a charm would use to manipulate a vector illustrator file and design a charm design. You need to follow the 70/30 Ruel"; the lead's D3: "blinds" is read as
 * BLENDS, Illustrator's Blend tool, and "deforming lines" as the warp / envelope presets, free distort and perspective).
 *
 * What Illustrator does and what this does (the most-used 70%):
 *   BLEND (Object > Blend, the Blend tool W): the shapes between two shapes. Specified Steps or Specified Distance; along a straight line, or along a selected path
 *     (Replace Spine) with Align to Page / Align to Path. Illustrator matches anchor points; where the two have different counts it adds points, and so does this
 *     (the longest segment halved until the counts agree), then it starts the second shape at the anchor that fits the first best (or at the anchors the Blend tool
 *     was clicked on) and turns it the same way round. A compound shape blends subpath by subpath; one with no partner grows out of (or shrinks into) a point.
 *     The blend is a group: the two ends and the steps between them, every step a real path, with the settings on the group (blend:{...}) so Blend Options can make
 *     the steps again from its ends; Expand leaves a plain group of paths.
 *   WARP (Effect > Warp, Object > Envelope Distort > Make with Warp): the fifteen styles (Arc, Arc Lower, Arc Upper, Arch, Bulge, Shell Lower, Shell Upper, Flag,
 *     Wave, Fish, Rise, Fisheye, Inflate, Squeeze, Twist), Horizontal / Vertical, Bend and the two Distortions, from −100 to 100 %, previewed live on the selection.
 *     Illustrator's formulas are not published; each style here is a smooth map of the selection's box written out below (Arc is an exact annular sector, so the
 *     test can hold it to the circle). Words are turned into their outlines first, as Illustrator does when an envelope is expanded. The result is real paths, and
 *     the paths keep what they were made from (warp:{...}) so Warp again starts from the original until Expand drops it.
 *   FREE DISTORT and PERSPECTIVE DISTORT (the Free Transform tool's two modes): the selection's four corners dragged on the charm. Free Distort is the projective map
 *     of the box onto the four corners (straight lines stay straight, as Illustrator's do); a corner dragged past its neighbours (a quad that is not convex) falls
 *     back to the bilinear map. Perspective moves the partner corner the opposite way, so the box becomes a symmetrical trapezium.
 * CURVES STAY CURVES: a path is mapped segment by segment. Each segment's image is a cubic through the mapped end points with the mapped tangents (the map's own
 * derivative along the segment), checked at eight points against the true image and halved until it is within the tolerance, 0.01 mm by default, the laser's own
 * precision (a line that stays straight comes out a line). Nothing is flattened into polylines.
 * SPEED (common brief: "<16 ms of work" a frame): the live preview is worked out once per frame with only the latest settings (requestAnimationFrame), at a
 * tolerance of a third of a screen pixel (never finer than 0.01 mm), and the original paths are ghosted under it; OK makes the paths again at 0.01 mm. A selection
 * too big for one frame is previewed in a Worker made from this same file (a Blob URL, no extra file) while the last preview stays on screen.
 *
 * Loads in the page (window.CharmNestWarp, and it registers itself through the Charm Editor's extension points: CharmEditor.tools / commands / onOpen) and in node
 * (module.exports: the maths only, for tests/charm-nest/ce-warp.cjs).
 *
 *   CharmNestWarp.model.mapSubpaths(subpaths, f, tolPt)            f(x, y) -> [x', y'] any smooth map; curves fitted back to cubics within tolPt
 *   CharmNestWarp.model.warpFn(o, box)                             o { style, bend, h, v (each −1..1), vertical }; box [x0, y0, x1, y1] (pt) -> f
 *   CharmNestWarp.model.quadFn(box, quad)                          the box onto four corners [tl, tr, br, bl] (projective; bilinear when not convex) -> f
 *   CharmNestWarp.model.blend(a, b, o)                             a, b subpaths; o { steps | distance (pt), spine (subpaths), orient 'page'|'path', startA, startB }
 *                                                                    -> { steps:[{ t, subpaths }], n, aM, bM (the ends' moves onto the spine: SVG matrices) }
 */
(function (root, factory) {
  const api = factory(root, factory);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestWarp = api;
})(typeof self !== 'undefined' ? self : this, function (root, factory) {
  'use strict';
  const MM = 25.4 / 72, PT = 72 / 25.4;
  const TOL_MM = 0.01;                                   // (the laser's own precision: the brief's "about 0.01 mm")
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
  const R6 = v => { const r = Math.round(v * 1e6) / 1e6; return Object.is(r, -0) ? 0 : r; };
  const P6 = p => [R6(p[0]), R6(p[1])];
  const lerp = (a, b, t) => a + (b - a) * t;
  const lerpP = (a, b, t) => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
  const dist = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1]);
  const bez1 = (a0, a1, a2, a3, t) => { const u = 1 - t; return u * u * u * a0 + 3 * u * u * t * a1 + 3 * u * t * t * a2 + t * t * t * a3; };
  const bezP = (s, t) => [bez1(s[0][0], s[1][0], s[2][0], s[3][0], t), bez1(s[0][1], s[1][1], s[2][1], s[3][1], t)];
  const bezD = (s, t) => { const u = 1 - t; return [3 * (u * u * (s[1][0] - s[0][0]) + 2 * u * t * (s[2][0] - s[1][0]) + t * t * (s[3][0] - s[2][0])), 3 * (u * u * (s[1][1] - s[0][1]) + 2 * u * t * (s[2][1] - s[1][1]) + t * t * (s[3][1] - s[2][1]))]; };
  const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
  const ap = (m, p) => [m[0] * p[0] + m[2] * p[1] + m[4], m[1] * p[0] + m[3] * p[1] + m[5]];
  const I = [1, 0, 0, 1, 0, 0];

  /* ═══ 1 · a path through a smooth map: subdivide and fit (curves stay curves) ═══ */
  const SAMPLES = [0.0625, 0.1875, 0.3125, 0.4375, 0.5625, 0.6875, 0.8125, 0.9375];
  const MAXDEPTH = 14;
  /** Subpaths (ops ["m",[x,y]] ["l",[x,y]] ["c",p1,p2,p3] ["h"]) through f(x, y) -> [x', y']. Each segment's image is fitted with cubics whose end tangents are the map's
   *  own derivative along the segment (so neighbouring pieces meet smoothly), halved until every check point is within tol (pt) of the true image; a line whose image
   *  stays within tol of its chord is a line. stats (optional) counts the pieces made and the map's evaluations. */
  function mapSubpaths(sps, f, tol, stats) {
    tol = tol > 0 ? tol : TOL_MM * PT;
    const hh = 1e-4, tolIn = tol * 0.75;   // (the check is at eight points: held a quarter under, the curve between them is within tol too)
    const F = (x, y) => { if (stats) stats.evals++; const q = f(x, y); return q; };
    // the derivative of the image along v at p: a central difference of the map along v's direction, times |v|
    const D = (p, v) => {
      const L = Math.hypot(v[0], v[1]); if (!(L > 1e-12)) return [0, 0];
      const ux = v[0] / L * hh, uy = v[1] / L * hh, a = F(p[0] + ux, p[1] + uy), b = F(p[0] - ux, p[1] - uy);
      return [(a[0] - b[0]) / (2 * hh) * L, (a[1] - b[1]) / (2 * hh) * L];
    };
    const out = [];
    for (const sub of sps || []) {
      let o = [], cur = null, start = null, qcur = null, qstart = null;
      const moveTo = p => { if (o.length > 1) out.push(o); o = []; cur = p; start = p; qcur = F(p[0], p[1]); qstart = qcur; o.push(['m', P6(qcur)]); };
      const seg = (P, dP, isLine) => {
        const q0 = qcur, q1 = F(...P(1));
        if (![q0[0], q0[1], q1[0], q1[1]].every(fin)) { if (fin(q1[0]) && fin(q1[1])) { o.push(['l', P6(q1)]); qcur = q1; } return; }
        qcur = q1;
        if (isLine) {
          // a line whose image stays on its chord (an affine or a projective map, the radial sides of an arc) is still a line
          const cx = q1[0] - q0[0], cy = q1[1] - q0[1], cl = Math.hypot(cx, cy);
          let straight = cl > 1e-9;
          for (let i = 0; straight && i < SAMPLES.length; i++) {
            const q = F(...P(SAMPLES[i])), dx = q[0] - q0[0], dy = q[1] - q0[1];
            const off = Math.abs(dx * cy - dy * cx) / cl, along = (dx * cx + dy * cy) / (cl * cl);
            if (!(off <= tol * 0.5) || along < -1e-6 || along > 1 + 1e-6) straight = false;
          }
          if (straight || (cl <= 1e-9 && dist(F(...P(0.5)), q0) <= tol * 0.5)) { o.push(['l', P6(q1)]); return; }
        }
        const fit = (t0, t1, a, da, b, db, depth) => {
          const s = t1 - t0;
          const c1 = [a[0] + da[0] * s / 3, a[1] + da[1] * s / 3], c2 = [b[0] - db[0] * s / 3, b[1] - db[1] * s / 3];
          const cub = [a, c1, c2, b];
          let err = 0;
          for (const u of SAMPLES) { const tq = F(...P(t0 + u * s)), bq = bezP(cub, u); const e = Math.hypot(tq[0] - bq[0], tq[1] - bq[1]); if (!(e <= err)) err = fin(e) ? e : Infinity; if (err > tolIn) break; }
          if (err <= tolIn || depth >= MAXDEPTH) { if (stats) stats.pieces++; o.push(['c', P6(c1), P6(c2), P6(b)]); return; }
          const tm = (t0 + t1) / 2, pm = P(tm), qm = F(pm[0], pm[1]), dm = D(pm, dP(tm));
          fit(t0, tm, a, da, qm, dm, depth + 1); fit(tm, t1, qm, dm, b, db, depth + 1);
        };
        fit(0, 1, q0, D(P(0), dP(0)), q1, D(P(1), dP(1)), 0);
      };
      const lineTo = b => { const a = cur; seg(t => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t], () => [b[0] - a[0], b[1] - a[1]], true); cur = b; };
      for (const op of sub || []) {
        const k = op && op[0];
        if (k === 'm') moveTo(op[1]);
        else if (k === 'l') { if (!cur) moveTo(op[1]); else lineTo(op[1]); }
        else if (k === 'c') {
          if (!cur) moveTo(op[1]);
          const s = [cur, op[1], op[2], op[3]];
          seg(t => bezP(s, t), t => bezD(s, t), false);
          cur = op[3];
        } else if (k === 'h') {
          if (!cur) continue;
          if (dist(cur, start) > 1e-9) {
            const n0 = o.length; lineTo(start);
            // (a closing side that is still straight is left to the close, as it came in)
            if (o.length === n0 + 1 && o[n0][0] === 'l' && Math.hypot(o[n0][1][0] - R6(qstart[0]), o[n0][1][1] - R6(qstart[1])) < 1e-9) o.pop();
          }
          o.push(['h']); cur = start; qcur = qstart;
        }
      }
      if (o.length > 1) out.push(o);
    }
    return out;
  }

  /* ═══ 2 · the warp styles: each a smooth map of the box [x0, y0, x1, y1] (pt, y down), bend b from −1 to 1 (−100 % … 100 %) ═══
     u, v: where a point sits across and down the box (0 … 1); X, Y the same from the middle (−1 … 1). Positive Bend bows up (Illustrator's sign); Vertical turns the
     style a quarter (the box transposed), and the two Distortions are applied last, in the box's own axes: Horizontal makes the right side taller than the left (the
     left shorter), Vertical makes the top wider than the bottom. */
  const STYLES = [
    ['arc', 'Arc'], ['arcLower', 'Arc Lower'], ['arcUpper', 'Arc Upper'], ['arch', 'Arch'], ['bulge', 'Bulge'], ['shellLower', 'Shell Lower'], ['shellUpper', 'Shell Upper'],
    ['flag', 'Flag'], ['wave', 'Wave'], ['fish', 'Fish'], ['rise', 'Rise'], ['fisheye', 'Fisheye'], ['inflate', 'Inflate'], ['squeeze', 'Squeeze'], ['twist', 'Twist']
  ];
  const STYLE_NAME = Object.fromEntries(STYLES);
  function coreFn(style, b, box) {
    const x0 = box[0], y0 = box[1], x1 = box[2], y1 = box[3], W = Math.max(1e-9, x1 - x0), H = Math.max(1e-9, y1 - y0), cx = (x0 + x1) / 2, cy = (y0 + y1) / 2;
    const ab = Math.abs(b), sg = b < 0 ? -1 : 1;
    if (ab < 1e-9) return null;
    // a point at u along the circular arc through (x0, Y) and (x1, Y) that sweeps th radians, bowing up (dir −1) or down (dir 1)
    const arcPt = (u, Y, th, dir) => {
      if (th < 1e-7) return [x0 + u * W, Y];
      const R = (W / 2) / Math.sin(th / 2), phi = (u - 0.5) * th, yc = Y - dir * R * Math.cos(th / 2);
      return [cx + R * Math.sin(phi), yc + dir * R * Math.cos(phi)];
    };
    // how far a circular arc of sweep th over the box's width stands above its chord at u, as a share of its middle (a parabola when th is small)
    const prof = (u, th) => {
      const x = (u - 0.5) * W;
      if (th < 1e-4) return 1 - 4 * x * x / (W * W);
      const R = (W / 2) / Math.sin(th / 2), c = R * Math.cos(th / 2), top = R - c;
      return (Math.sqrt(Math.max(0, R * R - x * x)) - c) / top;
    };
    const sagAt = (u, th) => { if (th < 1e-7) return 0; const R = (W / 2) / Math.sin(th / 2); return prof(u, th) * (R - R * Math.cos(th / 2)); };
    const G = 0.18, E1 = Math.exp(-1 / G), bump = X => Math.max(0, (Math.exp(-X * X / G) - E1) / (1 - E1));   // 1 in the middle, 0 at the edges, smooth
    switch (style) {
      case 'arc': {
        // an annular sector: the middle of the box's height becomes an arc as long as the box is wide, sweeping b × 180°; every vertical a radius, every
        // horizontal a circle about one centre
        const th = ab * Math.PI, Rm = W / th;
        return (x, y) => {
          const u = (x - x0) / W, yy = sg > 0 ? y : 2 * cy - y;
          const r = Rm + (cy - yy), phi = (u - 0.5) * th;
          const X = cx + r * Math.sin(phi), Y = cy + Rm - r * Math.cos(phi);
          return [X, sg > 0 ? Y : 2 * cy - Y];
        };
      }
      case 'arcLower': { const th = ab * Math.PI / 2; return (x, y) => { const u = (x - x0) / W, v = (y - y0) / H; return lerpP([x, y0], arcPt(u, y1, th, sg), v); }; }
      case 'arcUpper': { const th = ab * Math.PI / 2; return (x, y) => { const u = (x - x0) / W, v = (y - y0) / H; return lerpP(arcPt(u, y0, th, -sg), [x, y1], v); }; }
      case 'arch': { const th = ab * Math.PI / 2; return (x, y) => [x, y - sg * sagAt((x - x0) / W, th)]; }
      case 'bulge': { const k = b < 0 ? 0.9 * b : b; return (x, y) => [x, cy + (y - cy) * (1 + k * prof((x - x0) / W, Math.PI / 2))]; }
      case 'shellLower': { const k = 0.95 * b; return (x, y) => [x, y0 + (y - y0) * (1 + k * (2 * Math.sin(Math.PI * clamp((x - x0) / W, 0, 1)) - 1))]; }
      case 'shellUpper': { const k = 0.95 * b; return (x, y) => [x, y1 - (y1 - y) * (1 + k * (2 * Math.sin(Math.PI * clamp((x - x0) / W, 0, 1)) - 1))]; }
      case 'flag': return (x, y) => [x, y - b * H / 2 * Math.sin(2 * Math.PI * (x - x0) / W)];
      case 'wave': return (x, y) => [x, y - b * H / 2 * Math.sin(2 * Math.PI * (x - x0) / W - ((y - y0) / H - 0.5) * Math.PI / 2)];
      case 'fish': return (x, y) => [x, cy + (y - cy) * (1 + 0.8 * b * Math.sin(1.5 * Math.PI * (x - x0) / W))];   // (a body two thirds along, the tail narrowing)
      case 'rise': return (x, y) => [x, y - b * H / 2 * Math.sin(Math.PI * ((x - x0) / W - 0.5))];
      case 'fisheye': return (x, y) => { const X = (x - cx) / (W / 2), Y = (y - cy) / (H / 2), w = bump(X); return [cx + (x - cx) * (1 + 0.3 * b * w * bump(Y * 0.7)), cy + (y - cy) * (1 + b * w)]; };
      case 'inflate': return (x, y) => { const X = (x - cx) / (W / 2), Y = (y - cy) / (H / 2); return [cx + (x - cx) * (1 + 0.45 * b * (1 - Y * Y)), cy + (y - cy) * (1 + 0.45 * b * (1 - X * X))]; };
      case 'squeeze': return (x, y) => { const X = (x - cx) / (W / 2), Y = (y - cy) / (H / 2); return [cx + (x - cx) * (1 - 0.45 * b * (1 - Y * Y)), cy + (y - cy) * (1 + 0.45 * b * (1 - X * X))]; };
      case 'twist': {
        const Dg = Math.hypot(W, H) / 2;
        return (x, y) => {
          const dx = x - cx, dy = y - cy, d = Math.hypot(dx, dy) / Dg, k = Math.max(0, 1 - d), a = b * Math.PI / 2 * k * k, c = Math.cos(a), s = Math.sin(a);
          return [cx + c * dx - s * dy, cy + s * dx + c * dy];
        };
      }
      default: return null;
    }
  }
  /** The whole warp: the style (turned a quarter when vertical), then the two distortions. Returns null when it would change nothing. */
  function warpFn(o, box) {
    o = o || {};
    const b = clamp(+o.bend || 0, -1, 1), dh = clamp(+o.h || 0, -1, 1), dv = clamp(+o.v || 0, -1, 1);
    if (!box || !(box[2] > box[0]) || !(box[3] > box[1])) return null;
    let core;
    if (o.vertical) { const c = coreFn(o.style, b, [box[1], box[0], box[3], box[2]]); core = c && ((x, y) => { const q = c(y, x); return [q[1], q[0]]; }); }
    else core = coreFn(o.style, b, box);
    if (!core && Math.abs(dh) < 1e-9 && Math.abs(dv) < 1e-9) return null;
    const cx = (box[0] + box[2]) / 2, cy = (box[1] + box[3]) / 2, hw = (box[2] - box[0]) / 2, hhh = (box[3] - box[1]) / 2;
    if (Math.abs(dh) < 1e-9 && Math.abs(dv) < 1e-9) return core;
    return (x, y) => {
      const q = core ? core(x, y) : [x, y], Xn = (q[0] - cx) / hw, Yn = (q[1] - cy) / hhh;
      return [cx + (q[0] - cx) * (1 - 0.5 * dv * Yn), cy + (q[1] - cy) * (1 + 0.5 * dh * Xn)];
    };
  }
  /** Subpaths warped within box (the selection's box, pt), within tol. */
  function warpSubpaths(sps, o, box, tol, stats) {
    const f = warpFn(o, box);
    return f ? mapSubpaths(sps, f, tol, stats) : sps;
  }

  /* ═══ 3 · free distort and perspective: the box onto four corners ═══ */
  /** The unit square onto q [tl, tr, br, bl] (Heckbert's square-to-quad): a projective map, so straight lines stay straight. */
  function squareToQuad(q) {
    const [p0, p1, p2, p3] = q;
    const dx1 = p1[0] - p2[0], dx2 = p3[0] - p2[0], dx3 = p0[0] - p1[0] + p2[0] - p3[0];
    const dy1 = p1[1] - p2[1], dy2 = p3[1] - p2[1], dy3 = p0[1] - p1[1] + p2[1] - p3[1];
    let g = 0, h = 0;
    if (Math.abs(dx3) > 1e-12 || Math.abs(dy3) > 1e-12) {
      const det = dx1 * dy2 - dx2 * dy1; if (Math.abs(det) < 1e-18) return null;
      g = (dx3 * dy2 - dx2 * dy3) / det; h = (dx1 * dy3 - dx3 * dy1) / det;
    }
    const a = p1[0] - p0[0] + g * p1[0], bb = p3[0] - p0[0] + h * p3[0], c = p0[0];
    const d = p1[1] - p0[1] + g * p1[1], e = p3[1] - p0[1] + h * p3[1], ff = p0[1];
    return (u, v) => { const w = g * u + h * v + 1; return [(a * u + bb * v + c) / w, (d * u + e * v + ff) / w]; };
  }
  /** Is the quad convex and turning one way (its corners in order)? Only then is the projective map one-to-one over the box. */
  function convex(q) {
    let sign = 0;
    for (let i = 0; i < 4; i++) {
      const a = q[i], b = q[(i + 1) % 4], c = q[(i + 2) % 4];
      const z = (b[0] - a[0]) * (c[1] - b[1]) - (b[1] - a[1]) * (c[0] - b[0]);
      if (Math.abs(z) < 1e-12) return false;
      const s = z > 0 ? 1 : -1; if (!sign) sign = s; else if (s !== sign) return false;
    }
    return true;
  }
  /** The box onto the quad [tl, tr, br, bl]: projective when the quad is convex, else bilinear (Illustrator lets a corner be dragged anywhere). */
  function quadFn(box, q) {
    const x0 = box[0], y0 = box[1], W = Math.max(1e-9, box[2] - box[0]), H = Math.max(1e-9, box[3] - box[1]);
    const sq = convex(q) ? squareToQuad(q) : null;
    if (sq) return (x, y) => sq((x - x0) / W, (y - y0) / H);
    return (x, y) => {
      const u = (x - x0) / W, v = (y - y0) / H;
      const top = lerpP(q[0], q[1], u), bot = lerpP(q[3], q[2], u);
      return lerpP(top, bot, v);
    };
  }
  const boxQuad = b => [[b[0], b[1]], [b[2], b[1]], [b[2], b[3]], [b[0], b[3]]];
  /** Perspective Distort: corner i dragged by dx, dy from quad q0. Mostly sideways: its partner on the same top or bottom edge moves the other way; mostly up or down:
   *  its partner on the same left or right edge does. */
  function perspectiveDrag(q0, i, dx, dy) {
    const q = q0.map(p => p.slice());
    if (Math.abs(dx) >= Math.abs(dy)) { const j = [1, 0, 3, 2][i]; q[i][0] += dx; q[j][0] -= dx; }
    else { const j = [3, 2, 1, 0][i]; q[i][1] += dy; q[j][1] -= dy; }
    return q;
  }

  /* ═══ 4 · blends ═══ */
  /** A subpath as cubic segments { closed, segs:[[p0, c1, c2, p1, isLine]] }; a line is a cubic with its handles on thirds (and remembers it was a line). */
  function cubicSub(sub) {
    const segs = []; let cur = null, start = null, closed = false;
    const line = (a, b) => [a, lerpP(a, b, 1 / 3), lerpP(a, b, 2 / 3), b, true];
    for (const op of sub || []) {
      const k = op && op[0];
      if (k === 'm') { cur = op[1]; start = cur; }
      else if (k === 'l') { if (cur) segs.push(line(cur, op[1])); else start = op[1]; cur = op[1]; }
      else if (k === 'c') { if (!cur) { cur = op[1]; start = cur; } segs.push([cur, op[1], op[2], op[3], false]); cur = op[3]; }
      else if (k === 'h') { if (cur && start && dist(cur, start) > 1e-9) segs.push(line(cur, start)); closed = true; cur = start; }
    }
    return { closed, segs, start: start || [0, 0] };
  }
  function subOf(cs) {
    if (!cs.segs.length) return [['m', P6(cs.start)]];
    const o = [['m', P6(cs.segs[0][0])]];
    for (const s of cs.segs) o.push(s[4] ? ['l', P6(s[3])] : ['c', P6(s[1]), P6(s[2]), P6(s[3])]);
    if (cs.closed) o.push(['h']);
    return o;
  }
  const segLen = s => dist(s[0], s[1]) + dist(s[1], s[2]) + dist(s[2], s[3]);
  function splitSeg(s, t) {
    const [p0, p1, p2, p3] = s, a = lerpP(p0, p1, t), b = lerpP(p1, p2, t), c = lerpP(p2, p3, t), d = lerpP(a, b, t), e = lerpP(b, c, t), m = lerpP(d, e, t);
    return [[p0, a, d, m, s[4]], [m, e, c, p3, s[4]]];
  }
  /** More segments, by halving the longest, until there are n (Illustrator adds anchor points the same way when two blended paths have different counts). */
  function growTo(cs, n) {
    const segs = cs.segs.slice();
    if (!segs.length) { const p = cs.start; while (segs.length < n) segs.push([p, p, p, p, true]); return { closed: cs.closed, segs, start: p }; }
    while (segs.length < n) {
      let bi = 0, bl = -1; for (let i = 0; i < segs.length; i++) { const l = segLen(segs[i]); if (l > bl) { bl = l; bi = i; } }
      const two = splitSeg(segs[bi], 0.5); segs.splice(bi, 1, two[0], two[1]);
    }
    return { closed: cs.closed, segs, start: cs.start };
  }
  const reverseCs = cs => ({ closed: cs.closed, start: cs.segs.length ? cs.segs[cs.segs.length - 1][3] : cs.start, segs: cs.segs.slice().reverse().map(s => [s[3], s[2], s[1], s[0], s[4]]) });
  const rotateCs = (cs, k) => { const n = cs.segs.length; if (!n || !k) return cs; k = ((k % n) + n) % n; const segs = cs.segs.slice(k).concat(cs.segs.slice(0, k)); return { closed: cs.closed, segs, start: segs[0][0] }; };
  function areaOf(cs) {   // signed area of the control polygon's anchors and handles (enough to tell the turn)
    let a = 0; const pts = [];
    for (const s of cs.segs) pts.push(s[0], s[1], s[2]);
    for (let i = 0; i < pts.length; i++) { const p = pts[i], q = pts[(i + 1) % pts.length]; a += p[0] * q[1] - q[0] * p[1]; }
    return a / 2;
  }
  function boxOfCs(list) {
    let b = null;
    for (const cs of list) for (const s of cs.segs.length ? cs.segs : [[cs.start, cs.start, cs.start, cs.start]]) for (let i = 0; i < 4; i++) {
      const p = s[i]; b = b ? [Math.min(b[0], p[0]), Math.min(b[1], p[1]), Math.max(b[2], p[0]), Math.max(b[3], p[1])] : [p[0], p[1], p[0], p[1]];
    }
    return b || [0, 0, 0, 0];
  }
  const centreOfCs = cs => { const b = boxOfCs([cs]); return [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2]; };
  /* Anchors matched by how far round they are. Every anchor's place along its subpath is a share of the subpath's length (0 … 1); the second subpath is started at
     the place that makes its shape fit the first best (48 points of each compared from their own middles, at their own size: a square blended into a circle meets
     it corner to 45°, without the half-turn twist that matching anchor to anchor gives), or at the anchors the Blend tool was clicked on; then each gets an anchor
     wherever the other has one, so the two have the same anchors in the same places and every step is the straight way between them. */
  const LN = 16;
  function lenTab(cs) {
    const tabs = cs.segs.map(s => { const c = [0]; let prev = s[0]; for (let i = 1; i <= LN; i++) { const p = bezP(s, i / LN); c.push(c[i - 1] + dist(prev, p)); prev = p; } return c; });
    const starts = [0]; tabs.forEach((t, i) => starts.push(starts[i] + t[LN]));
    return { tabs, starts, total: starts[starts.length - 1] };
  }
  /** The t on a segment at length l along it (from its chord table). */
  function tAt(tab, l) {
    if (l <= 0) return 0; if (l >= tab[LN]) return 1;
    let i = 1; while (i < LN && tab[i] < l) i++;
    const a = tab[i - 1], b = tab[i]; return (i - 1 + (b > a ? (l - a) / (b - a) : 0)) / LN;
  }
  function pointAtFrac(cs, L, f) {
    f = ((f % 1) + 1) % 1; const l = f * L.total;
    let i = 0; while (i < cs.segs.length - 1 && L.starts[i + 1] <= l) i++;
    return bezP(cs.segs[i], tAt(L.tabs[i], l - L.starts[i]));
  }
  const anchorFracs = (cs, L) => cs.segs.map((_, i) => (L.total > 0 ? L.starts[i] / L.total : i / cs.segs.length));
  /** New anchors at these shares of the length (those within eps of an anchor already there are left). */
  function splitAtFracs(cs, L, fs, eps) {
    if (!(L.total > 0)) return cs;
    const segs = [];
    cs.segs.forEach((s, i) => {
      const a = L.starts[i] / L.total, b = L.starts[i + 1] / L.total;
      const here = fs.filter(f => f > a + eps && f < b - eps).sort((p, q) => p - q);
      let rest = s, t0 = 0;
      for (const f of here) {
        const t = tAt(L.tabs[i], (f - a) * L.total), lt = (t - t0) / (1 - t0);
        if (!(lt > 1e-9 && lt < 1 - 1e-9)) continue;
        const two = splitSeg(rest, lt); segs.push(two[0]); rest = two[1]; t0 = t;
      }
      segs.push(rest);
    });
    return { closed: cs.closed, segs, start: segs.length ? segs[0][0] : cs.start };
  }
  /** A closed subpath restarted at share f of its length (a new anchor there when there is none). */
  function restartAt(cs, L, f) {
    const eps = 1e-6, fr = anchorFracs(cs, L);
    let k = fr.findIndex(x => Math.abs(x - f) < eps);
    if (k < 0) { cs = splitAtFracs(cs, L, [f], eps); k = anchorFracs(cs, lenTab(cs)).findIndex(x => Math.abs(x - f) < 1e-4); }
    return k > 0 ? rotateCs(cs, k) : cs;
  }
  const nearestFrac = (cs, L, p) => { let bf = 0, bd = Infinity; const fr = anchorFracs(cs, L); cs.segs.forEach((s, i) => { const d = dist(s[0], p); if (d < bd) { bd = d; bf = fr[i]; } }); return bf; };
  /** One pair made to match: the same turn, the best start (or the clicked ones), then the same anchors in the same places. */
  function matchPair(a, b, startA, startB) {
    if (a.closed && b.closed) {
      const sa = areaOf(a), sb = areaOf(b);
      if (Math.abs(sa) > 1e-12 && Math.abs(sb) > 1e-12 && Math.sign(sa) !== Math.sign(sb)) b = reverseCs(b);
    }
    let La = lenTab(a), Lb = lenTab(b);
    if (!(La.total > 1e-12) || !(Lb.total > 1e-12)) { const n = Math.max(a.segs.length, b.segs.length, 1); return [growTo(a, n), growTo(b, n)]; }
    const N = 48;
    const ba = boxOfCs([a]), bb = boxOfCs([b]);
    const ca = [(ba[0] + ba[2]) / 2, (ba[1] + ba[3]) / 2], cb = [(bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2];
    const za = Math.max(ba[2] - ba[0], ba[3] - ba[1], 1e-9), zb = Math.max(bb[2] - bb[0], bb[3] - bb[1], 1e-9);
    const SA = [], SB = [];
    for (let k = 0; k < N; k++) { const p = pointAtFrac(a, La, k / N); SA.push([(p[0] - ca[0]) / za, (p[1] - ca[1]) / za]); }
    for (let k = 0; k < 2 * N; k++) { const p = pointAtFrac(b, Lb, k / (2 * N)); SB.push([(p[0] - cb[0]) / zb, (p[1] - cb[1]) / zb]); }
    const sampleB = (f, rev) => { if (rev) f = 1 - f; const p = pointAtFrac(b, Lb, f); return [(p[0] - cb[0]) / zb, (p[1] - cb[1]) / zb]; };
    if (a.closed && b.closed) {
      let off = 0;
      if (startA && startB) { a = restartAt(a, La, nearestFrac(a, La, startA)); La = lenTab(a); off = nearestFrac(b, Lb, startB); }
      else {
        const cands = anchorFracs(b, Lb).concat(Array.from({ length: 2 * N }, (_, k) => k / (2 * N)));
        let bc = Infinity;
        for (const o of cands) { let c = 0; for (let k = 0; k < N && c < bc; k++) { const q = sampleB(o + k / N), p = SA[k]; c += (p[0] - q[0]) ** 2 + (p[1] - q[1]) ** 2; } if (c < bc) { bc = c; off = o; } }
      }
      b = restartAt(b, Lb, off); Lb = lenTab(b);
    } else {
      // open: end to end, or the second turned round if that fits better
      let cf = 0, cr = 0;
      for (let k = 0; k <= N; k++) { const p = k < N ? SA[k] : (q => [(q[0] - ca[0]) / za, (q[1] - ca[1]) / za])(a.segs[a.segs.length - 1][3]); const f = k / N, q1 = f < 1 ? sampleB(f) : sampleB(0.999999), q2 = sampleB(f, true); cf += (p[0] - q1[0]) ** 2 + (p[1] - q1[1]) ** 2; cr += (p[0] - q2[0]) ** 2 + (p[1] - q2[1]) ** 2; }
      if (cr < cf) { b = reverseCs(b); Lb = lenTab(b); }
    }
    const fa = anchorFracs(a, La), fb = anchorFracs(b, Lb), eps = 1e-4;
    const a2 = splitAtFracs(a, La, fb, eps), b2 = splitAtFracs(b, Lb, fa, eps);
    if (a2.segs.length === b2.segs.length) return [a2, b2];
    const n = Math.max(a2.segs.length, b2.segs.length);   // (two anchors closer than eps on one side: the counts are made equal the plain way)
    return [growTo(a2, n), growTo(b2, n)];
  }
  /** Pairs of subpaths of shapes A and B, each pair with the same number of segments, started and turned alike. Subpaths are paired by where they sit in their
   *  shape and how much of it they are (a hole with a hole); one with no partner is paired with a point where it would be in the other shape. */
  function pairUp(A, B, startA, startB) {
    const ba = boxOfCs(A), bb = boxOfCs(B);
    const nrm = (p, b) => [(p[0] - b[0]) / Math.max(1e-9, b[2] - b[0]), (p[1] - b[1]) / Math.max(1e-9, b[3] - b[1])];
    const den = (p, b) => [b[0] + p[0] * (b[2] - b[0]), b[1] + p[1] * (b[3] - b[1])];
    const keyOf = (list, b) => { const tot = list.reduce((s, cs) => s + Math.abs(areaOf(cs)), 0) || 1; return list.map(cs => ({ c: nrm(centreOfCs(cs), b), a: Math.abs(areaOf(cs)) / tot })); };
    const kA = keyOf(A, ba), kB = keyOf(B, bb);
    const cand = [];
    A.forEach((_, i) => B.forEach((__, j) => cand.push([(kA[i].c[0] - kB[j].c[0]) ** 2 + (kA[i].c[1] - kB[j].c[1]) ** 2 + (kA[i].a - kB[j].a) ** 2, i, j])));
    cand.sort((p, q) => p[0] - q[0]);
    const ia = new Map(), usedB = new Set();
    for (const [, i, j] of cand) if (!ia.has(i) && !usedB.has(j)) { ia.set(i, j); usedB.add(j); }
    const point = (cs, from, to) => { const p = den(nrm(centreOfCs(cs), from), to); return { closed: cs.closed, segs: cs.segs.map(() => [p, p, p, p, true]), start: p }; };
    const pairs = [];
    const fit = (a, b, first) => matchPair(a, b, first ? startA : null, first ? startB : null);
    // (the clicked anchors of the Blend tool belong to the biggest pair: the outline a person clicks on)
    let firstI = -1, fa = -1; A.forEach((cs, i) => { const s = Math.abs(areaOf(cs)); if (ia.has(i) && s > fa) { fa = s; firstI = i; } });
    A.forEach((cs, i) => { if (ia.has(i)) pairs.push(fit(cs, B[ia.get(i)], i === firstI)); else pairs.push([cs, point(cs, ba, bb)]); });
    B.forEach((cs, j) => { if (!usedB.has(j)) pairs.push([point(cs, bb, ba), cs]); });
    return pairs;
  }
  /** The shape at t between the pairs (every anchor and handle straight from one to the other). */
  function lerpPairs(pairs, t, M) {
    return pairs.map(([a, b]) => {
      const segs = a.segs.map((s, i) => { const r = b.segs[i]; const q = [0, 1, 2, 3].map(k => lerpP(s[k], r[k], t)); if (M) for (let k = 0; k < 4; k++) q[k] = ap(M, q[k]); q.push(!!(s[4] && r[4])); return q; });
      const st = M ? ap(M, lerpP(a.start, b.start, t)) : lerpP(a.start, b.start, t);
      return subOf({ closed: a.closed && b.closed, segs, start: st });
    });
  }
  /** A path walked by length: its first subpath cut into short chords, so a point and its direction can be found at any share of the way. */
  function walker(sps) {
    const sub = (sps || []).find(s => s && s.length > 1);
    if (!sub) return null;
    const cs = cubicSub(sub); if (!cs.segs.length) return null;
    const pts = [cs.segs[0][0]];
    for (const s of cs.segs) { const n = s[4] ? 1 : Math.max(8, Math.min(200, Math.ceil(segLen(s) / 0.25))); for (let i = 1; i <= n; i++) pts.push(bezP(s, i / n)); }
    const cum = [0]; for (let i = 1; i < pts.length; i++) cum.push(cum[i - 1] + dist(pts[i - 1], pts[i]));
    const L = cum[cum.length - 1];
    const at = f => {
      const s = clamp(f, 0, 1) * L; let lo = 0, hi = cum.length - 1;
      while (hi - lo > 1) { const m = (lo + hi) >> 1; if (cum[m] <= s) lo = m; else hi = m; }
      const span = cum[hi] - cum[lo] || 1, k = (s - cum[lo]) / span, a = pts[lo], b = pts[hi];
      return { p: lerpP(a, b, k), angle: Math.atan2(b[1] - a[1], b[0] - a[0]) };
    };
    return { length: L, at, start: pts[0], end: pts[pts.length - 1] };
  }
  /** The steps of a blend from shape a to shape b (subpaths, face space).
   *  o.steps (default 5) or o.distance (pt between steps, measured centre to centre along the way); o.spine: subpaths of a path to follow (the ends move onto its
   *  two ends); o.orient 'path' turns every shape with the spine's direction; o.startA / o.startB: points the Blend tool was clicked at (the anchors to match). */
  function blend(a, b, o) {
    o = o || {};
    const A = (a || []).map(cubicSub).filter(cs => cs.segs.length), B = (b || []).map(cubicSub).filter(cs => cs.segs.length);
    if (!A.length || !B.length) return { steps: [], n: 0, aM: I, bM: I };
    const ba = boxOfCs(A), bb = boxOfCs(B), ca = [(ba[0] + ba[2]) / 2, (ba[1] + ba[3]) / 2], cb = [(bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2];
    let sp = o.spine ? walker(o.spine) : null;
    if (sp && sp.length < 1e-6) sp = null;
    let rev = false;
    if (sp && dist(sp.start, cb) + dist(sp.end, ca) < dist(sp.start, ca) + dist(sp.end, cb)) rev = true;   // the spine runs from a to b
    const walk = sp && (f => { const r = sp.at(rev ? 1 - f : f); return rev ? { p: r.p, angle: r.angle + Math.PI } : r; });
    const L = sp ? sp.length : dist(ca, cb);
    let n = Math.round(+o.steps);
    if (o.distance > 0) n = Math.round(L / o.distance) - 1;
    n = clamp(fin(n) ? n : 5, 0, 1000);
    // with a spine both shapes are drawn about their own middles, and each step is put on the spine (turned with it when aligned to the path)
    let pairs, Mof;
    if (walk) {
      const toO = (list, c) => list.map(cs => ({ closed: cs.closed, start: [cs.start[0] - c[0], cs.start[1] - c[1]], segs: cs.segs.map(s => [0, 1, 2, 3].map(k => [s[k][0] - c[0], s[k][1] - c[1]]).concat([s[4]])) }));
      pairs = pairUp(toO(A, ca), toO(B, cb), o.startA && [o.startA[0] - ca[0], o.startA[1] - ca[1]], o.startB && [o.startB[0] - cb[0], o.startB[1] - cb[1]]);
      const a0 = walk(0).angle;
      Mof = t => { const r = walk(t), d = o.orient === 'path' ? r.angle - a0 : 0, c = Math.cos(d), s = Math.sin(d); return [c, s, -s, c, r.p[0], r.p[1]]; };
    } else pairs = pairUp(A, B, o.startA, o.startB);
    const steps = [];
    for (let i = 1; i <= n; i++) { const t = i / (n + 1); steps.push({ t, subpaths: lerpPairs(pairs, t, Mof ? Mof(t) : null) }); }
    const aM = Mof ? mul(Mof(0), [1, 0, 0, 1, -ca[0], -ca[1]]) : I, bM = Mof ? mul(Mof(1), [1, 0, 0, 1, -cb[0], -cb[1]]) : I;
    return { steps, n, aM, bM, length: L };
  }

  /** One piece of work as plain data (what the Worker is sent): { sps, kind: 'warp' | 'quad', o, box, quad, tol, P } -> subpaths. P: the frame the subpaths are in
   *  (the map is made in face space and the result handed back in that frame). */
  function runJob(j) {
    let f = j.kind === 'quad' ? quadFn(j.box, j.quad) : warpFn(j.o, j.box);
    if (!f) return j.sps;
    if (j.P) {
      const P = j.P, det = P[0] * P[3] - P[1] * P[2];
      const Pi = [P[3] / det, -P[1] / det, -P[2] / det, P[0] / det, 0, 0]; Pi[4] = -(Pi[0] * P[4] + Pi[2] * P[5]); Pi[5] = -(Pi[1] * P[4] + Pi[3] * P[5]);
      const g = f; f = (x, y) => ap(Pi, g(...ap(P, [x, y])));
    }
    return mapSubpaths(j.sps, f, j.tol);
  }

  const model = { MM, PT, TOL_MM, STYLES, STYLE_NAME, mapSubpaths, warpFn, warpSubpaths, quadFn, squareToQuad, convex, boxQuad, perspectiveDrag, blend, cubicSub, walker, splitSeg, runJob };
  const api = { model, version: 1 };
  if (!root || !root.document) return api;   // node, or a Worker: the maths only

  /* ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
     5 · IN THE CHARM EDITOR (its extension points: CharmEditor.tools, .commands, .onClose; the ctx each is handed is described in its header)
     ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  const doc = root.document, SVGNS = 'http://www.w3.org/2000/svg';
  const TOL = TOL_MM * PT, HEAVY = 2500;            // (ops in a selection past which the work goes to the Worker: about 10 ms of it on a busy machine)
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const cssEsc = s => (root.CSS && root.CSS.escape ? root.CSS.escape(String(s)) : String(s).replace(/["\\]/g, '\\$&'));
  const trim = (v, dp) => String(+(+v).toFixed(dp));
  const log = (kind, data) => { try { if (root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event(kind, data); } catch (e) { /* the log is a courtesy */ } };
  const now = () => (root.performance && root.performance.now ? root.performance.now() : Date.now());
  const raf = fn => (root.requestAnimationFrame ? root.requestAnimationFrame(fn) : setTimeout(fn, 16));
  const pxPerPt = ctx => (+ctx.zoom() || 100) / 100 * 96 / 72;
  const opsIn = sps => { let n = 0; for (const s of sps || []) n += s.length; return n; };

  /* the icons, in the editor's hand: 24-unit box, 1.6 stroke, round ends */
  const SV = d => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${d}</svg>`;
  const IC = {
    blend: SV('<rect x="2.5" y="8" width="7" height="7" rx=".6"/><rect x="8.2" y="7.6" width="7.6" height="7.6" rx="2.6" stroke-dasharray="1.4 1.7" opacity=".7"/><circle cx="17.6" cy="11.6" r="3.9"/>'),
    warp: SV('<path d="M3.5 15.5C6 8.5 18 8.5 20.5 15.5"/><path d="M7 19.5c2.6-3.4 7.4-3.4 10 0"/><path d="m3.5 15.5 3.5 4M20.5 15.5l-3.5 4"/>'),
    distort: SV('<path d="M5 5.5 18.5 3.5 20.5 19 4 17.5Z"/><rect x="3.4" y="3.9" width="3.2" height="3.2" fill="currentColor" stroke="none"/><rect x="16.9" y="1.9" width="3.2" height="3.2" fill="currentColor" stroke="none"/><rect x="18.9" y="17.4" width="3.2" height="3.2" fill="currentColor" stroke="none"/><rect x="2.4" y="15.9" width="3.2" height="3.2" fill="currentColor" stroke="none"/>'),
    perspective: SV('<path d="M8.5 5h7l5.5 14H3Z"/><path d="M12 5v14" stroke-dasharray="1.6 1.8" opacity=".7"/>'),
    expand: SV('<path d="M4 9V4h5M15 4h5v5M20 15v5h-5M9 20H4v-5"/><path d="M8.5 12h7M12 8.5v7"/>')
  };

  const CSS = `
.ce-root .cw-panel{ position:absolute; z-index:17; width:min(286px,calc(100% - 24px)); overflow-y:auto; background:#fff; border:1px solid var(--ink); box-shadow:0 18px 44px rgba(28,29,29,.22);
  padding:13px 14px 12px; font-size:12px; color:var(--ink); animation:cwIn .16s cubic-bezier(.16,1,.3,1); }
@keyframes cwIn{ from{ opacity:0; transform:translateY(-4px); } to{ opacity:1; transform:none; } }
.ce-root .cw-head{ display:flex; align-items:baseline; gap:10px; margin:0 0 10px; padding-bottom:8px; border-bottom:1px solid var(--hair); min-width:0; }
.ce-root .cw-head h3{ margin:0; font-family:var(--serif); font-weight:500; font-size:21px; line-height:1.1; flex:none; }
.ce-root .cw-sub{ font-size:9px; font-weight:700; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; min-width:0; }
.ce-root .cw-row{ display:flex; align-items:center; gap:8px; margin:0 0 8px; min-height:30px; }
.ce-root .cw-row[hidden]{ display:none; }
.ce-root .cw-lab{ flex:0 0 74px; font-size:9.5px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--muted); }
.ce-root .cw-sec{ font-size:9px; font-weight:700; letter-spacing:.18em; text-transform:uppercase; color:var(--muted); margin:10px 0 6px; padding-top:8px; border-top:1px solid var(--hair); }
.ce-root .cw-panel select{ flex:1 1 auto; min-width:0; height:30px; border:1px solid var(--hair); border-radius:0; background:#fff; font:inherit; font-size:12px; color:var(--ink); padding:0 6px; }
.ce-root .cw-panel select:hover{ border-color:var(--hov-line); }
.ce-root .cw-panel input[type=range]{ flex:1 1 auto; min-width:0; accent-color:var(--gold); height:20px; }
.ce-root .cw-num{ flex:none; display:inline-flex; align-items:center; gap:3px; height:28px; padding:0 6px; border:1px solid var(--hair); background:#fff; }
.ce-root .cw-num input{ width:36px; border:0; padding:0; outline:none; background:none; font:inherit; font-size:11px; font-weight:600; color:var(--ink); text-align:right; }
.ce-root .cw-num i{ font-style:normal; font-size:10px; font-weight:800; color:var(--muted); }
.ce-root .cw-num:focus-within{ background:var(--band); }
.ce-root .cw-panel .ce-seg{ flex:1 1 auto; height:28px; }
.ce-root .cw-panel .ce-seg button{ flex:1 1 0; padding:0 6px; letter-spacing:.1em; }
.ce-root .cw-val{ flex:1 1 auto; min-width:0; font-size:11.5px; color:var(--ink); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.ce-root .cw-foot{ display:flex; align-items:center; gap:8px; margin-top:12px; padding-top:10px; border-top:1px solid var(--hair); }
.ce-root .cw-check{ display:inline-flex; align-items:center; gap:6px; font-size:9.5px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--muted); cursor:pointer; margin-right:auto; }
.ce-root .cw-check input{ accent-color:var(--gold); margin:0; }
.ce-root .cw-foot .ce-btn{ padding:9px 14px; letter-spacing:.2em !important; font-size:10.5px; }
.ce-root .cw-busy{ font-size:10.5px; color:var(--muted); min-height:14px; margin-top:6px; }
.ce-root .cw-shield{ position:absolute; inset:0; z-index:5; cursor:default; }
.ce-art .cw-prev .ce-p{ opacity:.96; }
.ce-ui .cw-quad{ fill:rgba(165,138,82,.06); stroke:var(--gold); stroke-width:1; stroke-dasharray:5 3; }
.ce-ui .cw-pick{ fill:var(--gold); stroke:#fff; stroke-width:1.5; }
.ce-ui .cw-hov{ fill:#fff; stroke:var(--gold); stroke-width:1.5; }
@media (prefers-reduced-motion: reduce){ .ce-root .cw-panel{ animation:none; } }
`;
  function injectCss() {
    if (doc.getElementById('cwCss')) return;
    const s = doc.createElement('style'); s.id = 'cwCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }

  /* ── the Worker: this same file, made from its own source (a Blob URL, no extra file); a selection too big for one frame is mapped there ── */
  let worker = null, wSeq = 0;
  const wWait = new Map();
  function getWorker() {
    if (worker !== null) return worker || null;
    try {
      if (!root.Worker || !root.Blob || !root.URL || typeof factory !== 'function') throw new Error('no Worker');
      const src = 'self.CW=(' + factory.toString() + ')(self,null);self.onmessage=function(e){var d=e.data,r;try{r={id:d.id,out:d.jobs.map(function(j){return self.CW.model.runJob(j);})};}catch(er){r={id:d.id,error:String(er&&er.message||er)};}self.postMessage(r);};';
      worker = new root.Worker(root.URL.createObjectURL(new root.Blob([src], { type: 'text/javascript' })));
      worker.onmessage = e => { const d = e.data || {}, w = wWait.get(d.id); if (!w) return; wWait.delete(d.id); if (d.error) w.reject(new Error(d.error)); else { stats.fromWorker++; w.resolve(d.out); } };
      worker.onerror = () => { for (const w of wWait.values()) w.reject(new Error('worker failed')); wWait.clear(); worker = false; };
    } catch (e) { worker = false; }
    return worker || null;
  }
  /** The jobs done: on this thread when they are small (or there is no Worker), else in the Worker. */
  function runJobs(jobs, big) {
    const w = big ? getWorker() : null;
    if (!w) return Promise.resolve(jobs.map(runJob));
    return new Promise((resolve, reject) => { const id = ++wSeq; wWait.set(id, { resolve, reject }); w.postMessage({ id, jobs }); })
      .catch(() => jobs.map(runJob));
  }

  /* ── what is selected, as leaves in face space ── */
  /** The selection's unlocked top items, and every leaf under them with its face matrix and its outlines in face space (words as their outlines). */
  function gather(ctx) {
    const M = ctx.model, geo = ctx.geo, tops = [], leaves = [];
    for (const id of ctx.selection()) {
      const e = ctx.entry(id); if (!e || e.it.locked) continue;
      tops.push(e);
      const rec = (it, W, path) => {
        const W2 = M.mul(W, geo.matrix(it));
        if (it.kind === 'group') { for (const ch of it.children || []) rec(ch, W2, path.concat(it.id)); return; }
        if (it.locked) return;
        leaves.push({ top: e, it, W: W2, face: M.transformSubpaths(geo.raw(it).subpaths, W2), path });
      };
      rec(e.it, e.P || M.I, []);
    }
    let box = null; for (const l of leaves) box = M.union(box, M.bboxOf(l.face));
    return { tops, leaves, box, ops: leaves.reduce((n, l) => n + opsIn(l.face), 0) };
  }
  const isOpenPath = it => it && it.kind === 'path' && Array.isArray(it.subpaths) && it.subpaths.length > 0 && !it.subpaths.some(sp => sp.some(op => op && op[0] === 'h'));
  /** A leaf turned into a path with new geometry (a text becomes its outlines: it keeps its role and how it is painted, its words go). */
  function asPath(ctx, leaf, subpaths, extra) {
    const M = ctx.model, o = Object.assign({}, leaf, { kind: 'path', subpaths, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }, extra || {});
    if (leaf.kind === 'text') {
      o.fill = M.paintFill(leaf); o.stroke = M.paintStroke(leaf);
      if (o.name == null || o._autoName) o.name = String(leaf.text || 'Text').split('\n')[0].slice(0, 40) || 'Text';
      for (const k of ['text', 'fontKey', 'sizePt', 'align', 'lineGap', '_autoName']) delete o[k];
    }
    delete o.matrix; delete o.pivot; delete o.bbox;
    return o;
  }

  /* ── the live preview: the new drawing over the charm in its own group of the editor's SVG (pt, so it is crisp at any zoom), the originals ghosted under it ── */
  function previewLayer(ctx) {
    const art = ctx.stage && ctx.stage.querySelector('.ce-art');
    const g = doc.createElementNS(SVGNS, 'g'); g.setAttribute('class', 'cw-prev'); g.setAttribute('pointer-events', 'none');
    if (art) art.appendChild(g);
    let ghosts = [];
    const unghost = () => { for (const el of ghosts) el.style.opacity = ''; ghosts = []; };
    return {
      ghost(ids) {
        unghost();
        for (const id of ids) { const el = art && art.querySelector(`.ce-items [data-id="${cssEsc(id)}"]`); if (el) { el.style.opacity = '.16'; ghosts.push(el); } }
      },
      /** list: [{ sps, it (the item it stands for: its role, paint and width) }] */
      draw(list) {
        const M = ctx.model, t0 = now();
        let h = '';
        for (const { sps, it } of list) {
          const fill = M.paintFill(it), stroke = M.paintStroke(it);
          h += `<path class="ce-p ce-p--${esc(it.role || 'art')}${fill ? ' is-fill' : ''}${stroke ? ' is-stroke' : ''}${it.evenOdd || it.fillRule === 'evenodd' ? ' is-evenodd' : ''}" d="${M.dOf(sps) || 'M0 0'}" vector-effect="non-scaling-stroke" style="--lw:${+M.lwOf(it) || 0.25}"/>`;
        }
        g.innerHTML = h;
        stats.draws.push(+(now() - t0).toFixed(2)); if (stats.draws.length > 200) stats.draws.shift();
      },
      blank() { g.innerHTML = ''; unghost(); },
      clear() { unghost(); if (g.parentNode) g.parentNode.removeChild(g); }
    };
  }
  const stats = { frames: [], draws: [], worker: 0, fromWorker: 0 };   // (the main-thread ms of each preview frame and of each drawing of one, for the tests: work measured, not waited for)
  /** Work done once a frame with only the latest settings; a preview slower than 12 ms moves to the Worker for the rest of the session. */
  function frameRunner(work) {
    let pending = false, inFlight = false, again = false, slow = false, dead = false;
    const go = () => {
      pending = false; if (dead) return;
      if (inFlight) { again = true; return; }
      const t0 = now(); const r = work(slow), ms = now() - t0;
      stats.frames.push(+ms.toFixed(2)); if (stats.frames.length > 200) stats.frames.shift();
      if (r && typeof r.then === 'function') { inFlight = true; stats.worker++; r.then(() => { inFlight = false; if (again) { again = false; kick(); } }, () => { inFlight = false; }); }
      else if (ms > 12) slow = true;
    };
    const kick = () => { if (!pending && !dead) { pending = true; raf(go); } };
    return { kick, stop() { dead = true; } };
  }

  /* ── the floating options panel (the studio's card: ink border, serif title, small-caps labels), over the work area's top-right; while it is open the charm is
     shielded from clicks (the wheel still zooms and pans, Space too), Enter is OK and Esc is Cancel ── */
  let dlg = null;
  function openPanel(ctx, o) {
    injectCss();
    if (dlg) dlg.cancel();
    const win = ctx.root && ctx.root.querySelector('.ce-win'), stage = ctx.stage;
    if (!win || !stage) return null;
    const el = doc.createElement('div');
    el.className = 'cw-panel'; el.setAttribute('role', 'dialog'); el.setAttribute('aria-label', o.title);
    el.innerHTML = `<div class="cw-head"><h3>${esc(o.title)}</h3><span class="cw-sub" title="${esc(o.sub || '')}">${esc(o.sub || '')}</span></div>${o.body}`
      + `<div class="cw-busy" data-cw-busy aria-live="polite"></div>`
      + `<div class="cw-foot"><label class="cw-check"><input type="checkbox" data-cw="preview" checked>Preview</label>`
      + `<button class="ce-btn ce-btn--ghost" type="button" data-cw-act="cancel">Cancel</button><button class="ce-btn ce-btn--gold" type="button" data-cw-act="ok">OK</button></div>`;
    const shield = doc.createElement('div'); shield.className = 'cw-shield';
    shield.addEventListener('pointerdown', e => { e.stopPropagation(); e.preventDefault(); });
    stage.appendChild(shield);
    win.appendChild(el);
    // over the work area's top-right on a desk; on a phone across the bottom of the work area, clear of the Cancel / Save foot; never taller than the work area
    const place = () => {
      const w = win.getBoundingClientRect(), s = stage.getBoundingClientRect(), px = v => Math.round(v) + 'px';
      if (w.width <= 640) Object.assign(el.style, { left: px(s.left - w.left + 6), right: px(w.right - s.right + 6), top: 'auto', bottom: px(w.bottom - s.bottom + 6), width: 'auto' });
      else Object.assign(el.style, { top: px(s.top - w.top + 10), right: px(w.right - s.right + 10), left: 'auto', bottom: 'auto', width: '' });
      el.style.maxHeight = px(Math.max(160, s.height - 12));
    };
    place();
    root.addEventListener('resize', place);
    let closed = false;
    const end = ok => {
      if (closed) return; closed = true;
      root.removeEventListener('resize', place);
      if (el.parentNode) el.parentNode.removeChild(el); if (shield.parentNode) shield.parentNode.removeChild(shield);
      if (dlg === me) dlg = null;
      o.onEnd(ok);
    };
    const me = { el, ok: () => end(true), cancel: () => end(false), busy: t => { const b = el.querySelector('[data-cw-busy]'); if (b) b.textContent = t || ''; } };
    el.addEventListener('click', e => { const a = e.target.closest('[data-cw-act]'); if (a) { if (a.dataset.cwAct === 'ok') me.ok(); else me.cancel(); } });
    dlg = me;
    requestAnimationFrame(() => { const f = el.querySelector('select, input[type=range]'); if (f && !closed) try { f.focus({ preventScroll: true }); } catch (e) { /* fine */ } });
    return me;
  }
  // (registered when this file loads, so it runs BEFORE the editor's own key listener, which is added when a workspace opens: the panel is modal for the keys)
  if (root.addEventListener) root.addEventListener('keydown', e => {
    const d = dlg; if (!d) return;
    const k = e.key, inPanel = !!(e.target && e.target.closest && e.target.closest('.cw-panel'));
    if (k === 'Escape') { e.preventDefault(); e.stopImmediatePropagation(); d.cancel(); return; }
    if (k === 'Enter' && !(inPanel && e.target.tagName === 'BUTTON')) { e.preventDefault(); e.stopImmediatePropagation(); d.ok(); return; }
    if (inPanel || k === 'Tab') return;   // the panel's own fields type as fields do (the editor leaves fields alone)
    if (k === ' ' || e.code === 'Space') return;   // the hand
    const mod = e.metaKey || e.ctrlKey;
    if (mod && /^(Digit[012]|Numpad[012]|Equal|Minus|NumpadAdd|NumpadSubtract)$/.test(e.code || '')) return;   // the zoom
    e.stopImmediatePropagation(); if (mod) e.preventDefault();
  }, true);
  /** The panel follows the work: anything that changes the items under it (an undo from the rail, the other face) cancels it. */
  function watchItems(ctx, onChange) {
    const g = ctx.stage && ctx.stage.querySelector('.ce-items');
    if (!g || !root.MutationObserver) return () => {};
    const mo = new root.MutationObserver(() => onChange());
    mo.observe(g, { childList: true, subtree: true, attributes: true, attributeFilter: ['d', 'transform', 'display'] });
    return () => mo.disconnect();
  }
  const pctField = (name, label, v, min, max) => `<div class="cw-row"><span class="cw-lab">${esc(label)}</span><input type="range" min="${min}" max="${max}" step="1" value="${v}" data-cw="${name}" aria-label="${esc(label)}"><label class="cw-num"><input data-cw-n="${name}" value="${v}" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="${esc(label)}, per cent"><i>%</i></label></div>`;
  /** A range and its number kept together; onInput(name, value) on every change. */
  function wireRanges(el, onInput) {
    for (const r of el.querySelectorAll('input[type=range][data-cw]')) {
      const n = el.querySelector(`[data-cw-n="${r.dataset.cw}"]`);
      r.addEventListener('input', () => { if (n) n.value = r.value; onInput(r.dataset.cw, +r.value); });
      if (n) n.addEventListener('input', () => { const v = parseFloat(String(n.value).replace(',', '.').replace(/−/g, '-')); if (!fin(v)) return; r.value = String(clamp(v, +r.min, +r.max)); onInput(r.dataset.cw, clamp(v, +r.min, +r.max)); });
    }
  }

  /* ═══ 5a · WARP (Effect > Warp): fifteen styles, live on the selection ═══ */
  const lastWarp = { style: 'arc', bend: 50, h: 0, v: 0, vertical: false };
  /** The selection's leaves as the warp sees them. When every leaf was warped here before (it keeps warp:{...}), Warp starts again from what it was made from, in its
   *  own frame (so a warped part moved or turned since keeps its move); otherwise from what is drawn now, words as outlines, all in one envelope round the selection. */
  function warpPlan(ctx) {
    const g = gather(ctx);
    if (!g.leaves.length || !g.box || !(g.box[2] - g.box[0] > 1e-6) || !(g.box[3] - g.box[1] > 1e-6)) return null;
    const again = g.leaves.every(l => l.it.warp && Array.isArray(l.it.warp.src) && Array.isArray(l.it.warp.box));
    return Object.assign(g, { again });
  }
  const optsOf = s => ({ style: s.style, bend: s.bend / 100, h: s.h / 100, v: s.v / 100, vertical: !!s.vertical });
  /** The jobs for one setting: per leaf, the map made in face space (or, again, in its own frame), at tolerance tol. */
  function warpJobs(ctx, plan, s, tol) {
    const o = optsOf(s);
    return plan.leaves.map(l => (plan.again
      ? { kind: 'warp', sps: l.it.warp.src, o, box: l.it.warp.box, tol }
      : { kind: 'warp', sps: l.face, o, box: plan.box, tol }));
  }
  /** The new items: each selected top item with its leaves warped (a group keeps its shape, its own turn and scale pushed into the new geometry). */
  function warpApply(ctx, plan, s, outs) {
    const M = ctx.model, geo = ctx.geo, byLeaf = new Map(plan.leaves.map((l, i) => [l.it, { l, out: outs[i] }]));
    const meta = (src, box) => ({ style: s.style, bend: s.bend / 100, h: s.h / 100, v: s.v / 100, vertical: !!s.vertical, box: box.map(R6), src });
    const map = new Map();
    for (const top of plan.tops) {
      const P = top.P || M.I, Pi = M.inv(P) || M.I, flat = M.isI(P);
      const rec = (it, Wrel) => {
        if (it.kind === 'group') {
          if (plan.again) return Object.assign({}, it, { children: (it.children || []).map(ch => rec(ch, null)) });
          const W2 = M.mul(Wrel, geo.matrix(it));
          return Object.assign({}, it, { tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, children: (it.children || []).map(ch => rec(ch, W2)) });
        }
        const hit = byLeaf.get(it);
        if (!hit) return plan.again || !Wrel ? it : M.pushDown(it, null, geo, Wrel, true);   // (a locked part stays where it is drawn)
        if (plan.again) return M.retext(it, { subpaths: hit.out, warp: meta(it.warp.src, it.warp.box) }, geo);   // (same place on the charm: the editor's own re-anchoring)
        const geom = flat ? hit.out : M.transformSubpaths(hit.out, Pi);
        return asPath(ctx, it, geom, flat ? { warp: meta(hit.l.face, plan.box) } : {});
      };
      map.set(top.it.id, rec(top.it, M.I));
    }
    return map;
  }
  function openWarp(ctx) {
    const plan = warpPlan(ctx);
    if (!plan) { ctx.say('Select something to warp'); return Promise.resolve(); }
    const s = Object.assign({}, lastWarp);
    if (plan.again) { const w = plan.leaves[0].it.warp; Object.assign(s, { style: STYLE_NAME[w.style] ? w.style : 'arc', bend: Math.round((+w.bend || 0) * 100), h: Math.round((+w.h || 0) * 100), v: Math.round((+w.v || 0) * 100), vertical: !!w.vertical }); }
    const face0 = ctx.face(), items0 = ctx.items();
    const prev = previewLayer(ctx);
    let showing = true;
    return new Promise(resolve => {
      const styleOpts = STYLES.map(([k, n]) => `<option value="${k}"${k === s.style ? ' selected' : ''}>${esc(n)}</option>`).join('');
      const body = `<div class="cw-row"><span class="cw-lab">Style</span><select data-cw="style" aria-label="Style">${styleOpts}</select></div>`
        + `<div class="cw-row"><span class="cw-lab">Direction</span><div class="ce-seg" role="group" aria-label="Direction"><button type="button" data-cw-dir="h">Horizontal</button><button type="button" data-cw-dir="v">Vertical</button></div></div>`
        + pctField('bend', 'Bend', s.bend, -100, 100)
        + `<div class="cw-sec">Distortion</div>` + pctField('h', 'Horizontal', s.h, -100, 100) + pctField('v', 'Vertical', s.v, -100, 100);
      const n = plan.leaves.length, words = plan.leaves.some(l => l.it.kind === 'text');
      const sub = (plan.again ? 'Warped again · ' : '') + (n === 1 ? (words ? 'words as outlines' : 'one path') : `${n} paths${words ? ' · words as outlines' : ''}`);
      let unwatch = () => {};
      const runner = frameRunner(slow => {
        if (!showing) { prev.blank(); return null; }
        const tol = Math.max(TOL, 0.35 / pxPerPt(ctx)), jobs = warpJobs(ctx, plan, s, tol);
        const draw = outs => {
          if (!showing || !dlg) return;
          prev.ghost(plan.tops.map(t => t.it.id));
          prev.draw(outs.map((sps, i) => { const l = plan.leaves[i]; return { sps: plan.again ? ctx.model.transformSubpaths(sps, l.W) : sps, it: l.it }; }));
        };
        if (slow || plan.ops > HEAVY) return runJobs(jobs, true).then(draw);
        draw(jobs.map(runJob)); return null;
      });
      const panel = openPanel(ctx, {
        title: 'Warp', sub,
        body,
        onEnd: async ok => {
          runner.stop(); unwatch();
          if (!ok) { prev.clear(); resolve(); return; }
          Object.assign(lastWarp, s);
          try {
            if (plan.ops > HEAVY) ctx.say('Warping…', 0);
            const outs = await runJobs(warpJobs(ctx, plan, s, TOL), plan.ops > HEAVY);
            prev.clear();
            if (ctx.face() !== face0 || ctx.items() !== items0) { ctx.say('The charm changed: the warp was not applied'); resolve(); return; }
            ctx.replaceItems(warpApply(ctx, plan, s, outs));
            ctx.select(plan.tops.map(t => t.it.id));
            ctx.commit('Warp · ' + STYLE_NAME[s.style]);
            ctx.say(`${STYLE_NAME[s.style]} ${s.bend}%${plan.ops > HEAVY ? ' · done' : ''}`);
            log('designWarp', { style: s.style, bend: s.bend, h: s.h, v: s.v, vertical: !!s.vertical, parts: n, again: !!plan.again });
          } catch (er) { prev.clear(); ctx.say('The warp could not be made'); }
          resolve();
        }
      });
      if (!panel) { prev.clear(); resolve(); return; }
      const el = panel.el;
      const syncDir = () => el.querySelectorAll('[data-cw-dir]').forEach(b => { const on = (b.dataset.cwDir === 'v') === !!s.vertical; b.classList.toggle('is-on', on); b.setAttribute('aria-pressed', String(on)); });
      syncDir();
      el.querySelector('[data-cw="style"]').addEventListener('change', e => { s.style = e.target.value; runner.kick(); });
      el.addEventListener('click', e => { const b = e.target.closest('[data-cw-dir]'); if (b) { s.vertical = b.dataset.cwDir === 'v'; syncDir(); runner.kick(); } });
      wireRanges(el, (k, v) => { s[k] = v; runner.kick(); });
      el.querySelector('[data-cw="preview"]').addEventListener('change', e => { showing = !!e.target.checked; runner.kick(); });
      unwatch = watchItems(ctx, () => { if (ctx.face() !== face0 || ctx.items() !== items0) panel.cancel(); });
      runner.kick();
    });
  }
  /** Expand: the warped paths keep their drawing and forget what they were made from (Warp then bends what is drawn). */
  function expandWarp(ctx) {
    const map = new Map();
    for (const id of ctx.selection()) {
      const e = ctx.entry(id); if (!e) continue;
      const rec = it => {
        if (it.kind === 'group') { const ch = (it.children || []).map(rec); return ch.every((c, i) => c === it.children[i]) ? it : Object.assign({}, it, { children: ch }); }
        if (!it.warp) return it; const o = Object.assign({}, it); delete o.warp; return o;
      };
      const n = rec(e.it); if (n !== e.it) map.set(id, n);
    }
    if (map.size) { ctx.replaceItems(map); ctx.commit('Expand warp'); ctx.say('Expanded: plain paths'); }
  }
  const hasWarp = ctx => ctx.selection().some(id => { const e = ctx.entry(id); let any = false; const rec = it => { if (it.warp) any = true; if (it.kind === 'group') (it.children || []).forEach(rec); }; if (e) rec(e.it); return any; });

  /* ═══ 5b · BLEND (Object > Blend, the Blend tool): the steps between shapes ═══ */
  const lastBlend = { spacing: 'steps', steps: 5, distanceMm: 2, orient: 'page' };
  /** The blend the selection makes (or is): its ends in stacking order, the spine if there is one, and the group it replaces when it is a blend already. */
  function blendPlan(ctx, picks) {
    const M = ctx.model, geo = ctx.geo;
    const es = (picks ? picks.map(p => ctx.entry(p.id)) : ctx.selection().map(id => ctx.entry(id))).filter(e => e && !e.it.locked);
    const order = new Map(); let n = 0; M.walk(ctx.items(), it => order.set(it.id, n++));
    const z = e => order.get(e.it.id) || 0;
    // a blend selected (alone, or with an open path to be its new spine: Replace Spine)
    const groups = es.filter(e => e.it.kind === 'group' && e.it.blend && Array.isArray(e.it.blend.ends));
    if (groups.length === 1 && (es.length === 1 || (es.length === 2 && isOpenPath(es.find(e => e !== groups[0]).it)))) {
      const G = groups[0], W = M.mul(G.P || M.I, geo.matrix(G.it));
      const kids = G.it.children || [];
      const ends = G.it.blend.ends.map(id => kids.find(k => k.id === id)).filter(Boolean);
      if (ends.length < 2) return null;
      const spineKid = kids.find(k => k.blendSpine);
      const other = es.find(e => e !== G);
      let turn = +G.it.blend.turn || 0;
      const faceEnds = ends.map((it, i) => {
        let f = M.pushDown(it, null, geo, W, true);
        if (i === ends.length - 1 && turn) { const c = boxCentre(geo.drawn(f, M.I)); f = M.pushDown(f, M.about(M.rotM(-turn * 180 / Math.PI), c[0], c[1]), geo, M.I, true); }
        return f;
      });
      return { group: G, ends: faceEnds, spine: other ? M.outlinesOf(other.it, geo, other.P) : spineKid ? M.outlinesOf(spineKid, geo, W) : null, spineId: other ? other.it.id : null,
        settings: Object.assign({}, lastBlend, pickSettings(G.it.blend)) };
    }
    if (es.length < 2) return null;
    let spineE = null;
    if (es.length === 3) { const open = es.filter(e => isOpenPath(e.it)); if (open.length === 1) spineE = open[0]; }
    const ends = es.filter(e => e !== spineE).sort((a, b) => z(a) - z(b));
    return { group: null, endsE: ends, ends: ends.map(e => M.pushDown(e.it, null, geo, e.P || M.I, true)), spine: spineE ? M.outlinesOf(spineE.it, geo, spineE.P) : null, spineId: spineE ? spineE.it.id : null,
      settings: Object.assign({}, lastBlend), z: ends.map(z) };
  }
  const pickSettings = b => { const o = {}; for (const k of ['spacing', 'steps', 'distanceMm', 'orient']) if (b[k] != null) o[k] = b[k]; return o; };
  const boxCentre = b => (b ? [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2] : [0, 0]);
  /** The leaf a step is painted like: the end itself, or an end group's first leaf. */
  const paintLeaf = it => { if (it.kind !== 'group') return it; for (const ch of it.children || []) { const l = paintLeaf(ch); if (l) return l; } return null; };
  /** The blend group, every child in face space: [spine (hidden), end, steps …, end, steps …, end] bottom first; the steps are real paths. */
  function makeBlend(ctx, plan, s, picks) {
    const M = ctx.model, geo = ctx.geo, ends = plan.ends;
    const o = { orient: s.orient }; if (s.spacing === 'distance') o.distance = Math.max(0.01, +s.distanceMm || 1) * PT; else o.steps = clamp(Math.round(+s.steps || 1), 1, 500);
    const spine = ends.length === 2 ? plan.spine : null;
    const kids = [], previews = [];
    let turn = 0, moved = ends.slice();
    for (let i = 0; i + 1 < ends.length; i++) {
      const a = ends[i], b = ends[i + 1];
      const ka = picks && ends.length === 2 ? picks.find(k => k.id === a.id) : null, kb = ka ? picks.find(k => k.id === b.id) : null;
      const pa = ka && kb ? ka.p : null, pb = ka && kb ? kb.p : null;
      const r = blend(M.outlinesOf(a, geo, M.I), M.outlinesOf(b, geo, M.I), Object.assign({}, o, { spine, startA: pa, startB: pb }));
      if (spine) {
        moved[i] = M.isI(r.aM) ? a : M.pushDown(a, r.aM, geo, M.I, true);
        moved[i + 1] = M.isI(r.bM) ? b : M.pushDown(b, r.bM, geo, M.I, true);
        turn = Math.atan2(r.bM[1], r.bM[0]);
      }
      const la = paintLeaf(a) || a, lb = paintLeaf(b) || b, lw = (M.lwOf(la) + M.lwOf(lb)) / 2;
      const steps = r.steps.map((st, k) => {
        const step = { id: M.newId(), kind: 'path', role: la.role || 'art', source: 'added', name: `Blend step ${k + 1}`, hidden: false, locked: false, subpaths: st.subpaths,
          fill: M.paintFill(la), stroke: M.paintStroke(la), lwPt: R6(lw), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, blendStep: true };
        if (la.evenOdd || la.fillRule === 'evenodd') step.evenOdd = true;
        return step;
      });
      kids.push({ end: i }, ...steps); previews.push(...steps);
    }
    kids.push({ end: ends.length - 1 });
    const children = kids.map(k => (k.end != null ? moved[k.end] : k));
    if (spine) children.unshift({ id: M.newId(), kind: 'path', role: 'art', source: 'added', name: 'Spine', hidden: true, locked: false, stroke: true, fill: false, lwPt: 0.25, subpaths: spine, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, blendSpine: true });
    const roles = {}; for (const e of ends) { const l = paintLeaf(e) || e; roles[l.role] = (roles[l.role] || 0) + 1; }
    const base = plan.group ? plan.group.it : null;
    const G = Object.assign({ id: M.newId(), kind: 'group', role: Object.keys(roles).sort((p, q) => roles[q] - roles[p])[0] || 'art', source: 'added', name: 'Blend', hidden: false, locked: false },
      base ? { id: base.id, name: base.name, source: base.source, role: base.role } : {},
      { tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, children, blend: { ends: ends.map(e => e.id), spacing: s.spacing, steps: o.steps || null, distanceMm: s.spacing === 'distance' ? +s.distanceMm : null, orient: s.orient, turn: R6(turn) } });
    return { G, steps: previews, moved: spine ? moved : null };
  }
  function openBlend(ctx, picks) {
    const plan = blendPlan(ctx, picks);
    if (!plan) { ctx.say('Select two shapes to blend (and an open path to follow, if you like)'); return Promise.resolve(); }
    const M = ctx.model, s = plan.settings, face0 = ctx.face(), items0 = ctx.items();
    const prev = previewLayer(ctx);
    let showing = true;
    const label = it => String((it && (it.name || (it.kind === 'text' ? it.text : ''))) || (it && it.kind === 'group' ? 'Group' : 'Path')).split('\n')[0].slice(0, 24);
    return new Promise(resolve => {
      const steps = clamp(Math.round(+s.steps || 5), 1, 500);
      const body = `<div class="cw-row"><span class="cw-lab">Spacing</span><select data-cw="spacing" aria-label="Spacing"><option value="steps"${s.spacing !== 'distance' ? ' selected' : ''}>Specified steps</option><option value="distance"${s.spacing === 'distance' ? ' selected' : ''}>Specified distance</option></select></div>`
        + `<div class="cw-row" data-cw-when="steps"><span class="cw-lab">Steps</span><input type="range" min="1" max="60" step="1" value="${Math.min(60, steps)}" data-cw="steps" aria-label="Steps"><label class="cw-num"><input data-cw-n="steps" value="${steps}" inputmode="numeric" autocomplete="off" spellcheck="false" aria-label="Steps"></label></div>`
        + `<div class="cw-row" data-cw-when="distance"><span class="cw-lab">Distance</span><label class="cw-num"><input data-cw-n="distance" value="${trim(+s.distanceMm || 2, 2)}" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Distance between steps, mm"><i>mm</i></label></div>`
        + `<div class="cw-row"><span class="cw-lab">Follows</span><span class="cw-val">${plan.spine && plan.ends.length === 2 ? 'The path (its spine)' : 'A straight line'}</span></div>`
        + `<div class="cw-row"><span class="cw-lab">Turn</span><div class="ce-seg" role="group" aria-label="Orientation"><button type="button" data-cw-or="page"${plan.spine ? '' : ' disabled'}>With page</button><button type="button" data-cw-or="path"${plan.spine ? '' : ' disabled'}>With path</button></div></div>`;
      const sub = plan.ends.length === 2 ? `${label(plan.ends[0])} → ${label(plan.ends[1])}` : `${plan.ends.length} shapes`;
      let unwatch = () => {};
      const ghostIds = () => (plan.group ? [plan.group.it.id] : plan.spine ? plan.endsE.map(e => e.it.id) : []);
      const runner = frameRunner(() => {
        if (!showing) { prev.blank(); return null; }
        const r = makeBlend(ctx, plan, s, picks);
        const list = r.steps.map(st => ({ sps: st.subpaths, it: st }));
        if (r.moved || plan.group) for (const it of (r.moved || plan.ends)) list.push({ sps: M.outlinesOf(it, ctx.geo, M.I), it: paintLeaf(it) || it });
        prev.ghost(ghostIds()); prev.draw(list);
        if (panel) panel.busy(`${r.steps.length} step${r.steps.length === 1 ? '' : 's'}`);
        return null;
      });
      const panel = openPanel(ctx, {
        title: 'Blend', sub, body,
        onEnd: ok => {
          runner.stop(); unwatch(); prev.clear();
          if (!ok) { resolve(); return; }
          Object.assign(lastBlend, pickSettings(s));
          if (ctx.face() !== face0 || ctx.items() !== items0) { ctx.say('The charm changed: the blend was not made'); resolve(); return; }
          const r = makeBlend(ctx, plan, s, picks), G = r.G;
          if (plan.group) {
            const P = plan.group.P || M.I;
            const g2 = M.isI(P) ? G : M.pushDown(G, M.inv(P), ctx.geo, M.I, true);
            const map = new Map([[plan.group.it.id, g2]]); if (plan.spineId) map.set(plan.spineId, null);
            ctx.replaceItems(map);
          } else {
            const top = plan.endsE[plan.endsE.length - 1];
            if (M.isI(top.P || M.I)) {
              const map = new Map([[top.it.id, G]]); for (const e of plan.endsE.slice(0, -1)) map.set(e.it.id, null); if (plan.spineId) map.set(plan.spineId, null);
              ctx.replaceItems(map);
            } else { ctx.removeItems(plan.endsE.map(e => e.it.id).concat(plan.spineId ? [plan.spineId] : [])); ctx.addItems([G]); }
          }
          ctx.select([G.id]);
          ctx.commit(plan.group ? 'Blend options' : 'Blend');
          ctx.say(`Blend · ${r.steps.length} step${r.steps.length === 1 ? '' : 's'}`);
          log('designBlend', { steps: r.steps.length, spacing: s.spacing, distanceMm: s.spacing === 'distance' ? +s.distanceMm : null, ends: plan.ends.length, spine: !!plan.spine, orient: s.orient, again: !!plan.group });
          resolve();
        }
      });
      if (!panel) { prev.clear(); resolve(); return; }
      const el = panel.el;
      const syncRows = () => {
        el.querySelectorAll('[data-cw-when]').forEach(r => { r.hidden = r.dataset.cwWhen !== s.spacing; });
        el.querySelectorAll('[data-cw-or]').forEach(b => { const on = b.dataset.cwOr === s.orient; b.classList.toggle('is-on', on && !b.disabled); b.setAttribute('aria-pressed', String(on)); });
      };
      syncRows();
      el.querySelector('[data-cw="spacing"]').addEventListener('change', e => { s.spacing = e.target.value; syncRows(); runner.kick(); });
      el.addEventListener('click', e => { const b = e.target.closest('[data-cw-or]'); if (b && !b.disabled) { s.orient = b.dataset.cwOr; syncRows(); runner.kick(); } });
      wireRanges(el, (k, v) => { if (k === 'steps') s.steps = clamp(Math.round(v), 1, 500); runner.kick(); });
      // (the steps field takes more than the slider shows: Illustrator allows up to 1000; 500 here)
      const sf = el.querySelector('[data-cw-n="steps"]'); sf.addEventListener('input', () => { const v = parseInt(sf.value, 10); if (v > 0) { s.steps = clamp(v, 1, 500); runner.kick(); } });
      const df = el.querySelector('[data-cw-n="distance"]'); df.addEventListener('input', () => { const v = parseFloat(String(df.value).replace(',', '.')); if (v > 0) { s.distanceMm = v; runner.kick(); } });
      el.querySelector('[data-cw="preview"]').addEventListener('change', e => { showing = !!e.target.checked; runner.kick(); });
      unwatch = watchItems(ctx, () => { if (ctx.face() !== face0 || ctx.items() !== items0) panel.cancel(); });
      runner.kick();
    });
  }
  const selBlend = ctx => { const ids = ctx.selection(); if (ids.length !== 1) return null; const e = ctx.entry(ids[0]); return e && e.it.kind === 'group' && e.it.blend ? e : null; };
  /** Release: the ends back on their own, still grouped (and the spine a path again); the steps go. */
  function releaseBlend(ctx) {
    const G = selBlend(ctx); if (!G) return;
    const kids = (G.it.children || []).filter(k => !k.blendStep).map(k => { if (!k.blendSpine) return k; const n = Object.assign({}, k, { hidden: false, name: 'Path' }); delete n.blendSpine; return n; });
    const n = Object.assign({}, G.it, { children: kids, name: G.it.name === 'Blend' ? 'Released blend' : G.it.name }); delete n.blend;
    ctx.replaceItems(new Map([[G.it.id, n]])); ctx.select([G.it.id]); ctx.commit('Release blend'); ctx.say('Released: the ends, grouped');
  }
  /** Expand: a plain group of real paths (the steps stay, the settings and the spine go). */
  function expandBlend(ctx) {
    const G = selBlend(ctx); if (!G) return;
    const n = Object.assign({}, G.it, { name: G.it.name === 'Blend' ? 'Blend (expanded)' : G.it.name, children: (G.it.children || []).filter(k => !k.blendSpine).map(k => { if (!k.blendStep) return k; const o = Object.assign({}, k); delete o.blendStep; return o; }) });
    delete n.blend;
    ctx.replaceItems(new Map([[G.it.id, n]])); ctx.select([G.it.id]); ctx.commit('Expand blend'); ctx.say('Expanded: a group of paths');
  }
  function canBlend(ctx) {
    const es = ctx.selection().map(id => ctx.entry(id)).filter(e => e && !e.it.locked);
    return es.length >= 2 && !(es.length === 2 && es.some(e => e.it.kind === 'group' && e.it.blend));
  }

  /* ═══ 5c · the tools: Blend (W), Free Distort and Perspective Distort (one flyout) ═══ */
  const near = (ctx, p, x, y, px) => { const a = ctx.stageOf(p[0], p[1]), b = ctx.stageOf(x, y); return Math.hypot(a[0] - b[0], a[1] - b[1]) <= px; };
  function hitTop(ctx, p) { const M = ctx.model; return M.hitTest(ctx.items(), p.x, p.y, 4 / pxPerPt(ctx), ctx.geo, {}); }
  /** The anchor of an item nearest to a point (face space), for the Blend tool's marks. */
  function nearestAnchor(ctx, it, P, x, y) {
    let best = null, bd = Infinity;
    for (const sp of ctx.model.outlinesOf(it, ctx.geo, P || ctx.model.I)) for (const op of sp) { const q = op[0] === 'c' ? op[3] : op[1]; if (!q || op[0] === 'h') continue; const d = Math.hypot(q[0] - x, q[1] - y); if (d < bd) { bd = d; best = q; } }
    return best;
  }
  const blendTool = (() => {
    let first = null, hov = null;
    return {
      id: 'cw-blend', icon: IC.blend, title: 'Blend', key: 'W', cursor: 'crosshair', tip: 'The steps between two shapes',
      hint: 'Click the first shape, then the second · the steps appear to set · Esc starts again',
      onArm(ctx) { first = null; hov = null; },
      onDisarm() { first = null; hov = null; },
      onDown(ctx, ev, p) {
        if (!p || ev.button) return;
        const hit = hitTop(ctx, p);
        if (!hit || hit.locked) { ctx.say(first ? 'Click the second shape' : 'Click a shape to start from'); return; }
        const e = ctx.entry(hit.id), at = nearestAnchor(ctx, hit, e && e.P, p.x, p.y) || [p.x, p.y];
        if (!first || first.id === hit.id) { first = { id: hit.id, p: at }; ctx.select([hit.id]); ctx.say('Now click the second shape'); ctx.redraw(); return; }
        const picks = [first, { id: hit.id, p: at }]; first = null; hov = null;
        ctx.select(picks.map(k => k.id)); ctx.redraw();
        openBlend(ctx, picks);
      },
      onMove(ctx, ev, p) {
        if (!p || ev.buttons) return;
        const hit = hitTop(ctx, p), e = hit && ctx.entry(hit.id);
        const h = hit && !hit.locked ? nearestAnchor(ctx, hit, e && e.P, p.x, p.y) : null;
        if ((h && hov && h[0] === hov[0] && h[1] === hov[1]) || (!h && !hov)) return;
        hov = h; ctx.redraw();
      },
      onUp() {},
      onKey(ctx, ev) { if (ev.key === 'Escape' && first) { first = null; ctx.redraw(); ctx.say('Blend: start again'); return true; } return false; },
      overlay(ctx) {
        let h = '';
        if (hov) { const a = ctx.stageOf(hov[0], hov[1]); h += `<rect class="cw-hov" x="${(a[0] - 4).toFixed(1)}" y="${(a[1] - 4).toFixed(1)}" width="8" height="8"/>`; }
        if (first) { const a = ctx.stageOf(first.p[0], first.p[1]); h += `<circle class="cw-pick" cx="${a[0].toFixed(1)}" cy="${a[1].toFixed(1)}" r="5"/>`; }
        return h;
      }
    };
  })();
  /** Free Distort and Perspective Distort: the selection's four corners, each dragged; the paths follow at once (worked once a frame) and are made at 0.01 mm when
   *  the corner is let go. */
  function distortTool(id, persp) {
    let st = null;   // { key, box, quad, drag: { i, q0, p0, plan, runner, prev } }
    const sync = ctx => {
      if (st && st.drag) return;
      const key = ctx.face() + '|' + ctx.selection().join('|');
      if (st && st.key === key && st.items === ctx.items()) return;
      const M = ctx.model; let box = null;
      for (const sid of ctx.selection()) { const e = ctx.entry(sid); if (e && !e.it.locked) box = M.union(box, ctx.geo.drawn(e.it, e.P || M.I)); }
      st = { key, items: ctx.items(), box, quad: box && box[2] - box[0] > 1e-6 && box[3] - box[1] > 1e-6 ? boxQuad(box) : null };
    };
    const end = () => { if (st && st.drag) { st.drag.runner.stop(); st.drag.prev.clear(); st.drag = null; } };
    return {
      id, icon: persp ? IC.perspective : IC.distort, title: persp ? 'Perspective Distort' : 'Free Distort', flyout: 'cw-distort', cursor: 'default',
      tip: persp ? 'Corners in a symmetric slant' : 'Drag any corner',
      hint: persp ? 'Drag a corner: its partner moves the other way, as in perspective · click a shape to pick it' : 'Drag a corner anywhere: the paths follow, curves stay curves · click a shape to pick it',
      onArm(ctx) { st = null; sync(ctx); ctx.redraw(); },
      onDisarm() { end(); st = null; },
      onDown(ctx, ev, p) {
        if (!p || ev.button) return;
        sync(ctx);
        const i = st && st.quad ? st.quad.findIndex(q => near(ctx, q, p.x, p.y, 10)) : -1;
        if (i < 0) { const hit = hitTop(ctx, p); ctx.select(hit && !hit.locked ? [hit.id] : []); st = null; sync(ctx); ctx.redraw(); return; }
        const plan = gather(ctx); if (!plan.leaves.length) return;
        const prev = previewLayer(ctx), q0 = st.quad.map(q => q.slice());
        const drag = { i, q0, p0: [p.x, p.y], plan, prev, moved: false };
        drag.runner = frameRunner(slow => {
          const tol = Math.max(TOL, 0.35 / pxPerPt(ctx)), jobs = plan.leaves.map(l => ({ kind: 'quad', sps: l.face, box: st.box, quad: st.quad.map(q => q.slice()), tol }));
          const draw = outs => { if (!st || st.drag !== drag) return; prev.ghost(plan.tops.map(t => t.it.id)); prev.draw(outs.map((sps, k) => ({ sps, it: plan.leaves[k].it }))); };
          if (slow || plan.ops > HEAVY) return runJobs(jobs, true).then(draw);
          draw(jobs.map(runJob)); return null;
        });
        st.drag = drag;
      },
      onMove(ctx, ev, p) {
        if (!p) return;
        const d = st && st.drag;
        if (!d) { if (!ev.buttons) { const prevKey = st && st.key; sync(ctx); if (!st || st.key !== prevKey) ctx.redraw(); } return; }
        const dx = p.x - d.p0[0], dy = p.y - d.p0[1];
        if (!d.moved && Math.hypot(dx, dy) * pxPerPt(ctx) < 2) return;
        d.moved = true;
        if (persp) st.quad = perspectiveDrag(d.q0, d.i, dx, dy);
        else { st.quad = d.q0.map(q => q.slice()); st.quad[d.i] = [d.q0[d.i][0] + dx, d.q0[d.i][1] + dy]; }
        d.runner.kick(); ctx.redraw();
      },
      onUp(ctx) {
        const d = st && st.drag; if (!d) return;
        d.runner.stop();
        if (!d.moved) { d.prev.clear(); st.drag = null; return; }
        const quad = st.quad.map(q => q.slice()), box = st.box, plan = d.plan;
        const jobs = plan.leaves.map(l => ({ kind: 'quad', sps: l.face, box, quad, tol: TOL }));
        const finish = outs => {
          d.prev.clear(); if (st) st.drag = null;
          const M = ctx.model, geo = ctx.geo, byLeaf = new Map(plan.leaves.map((l, k) => [l.it, outs[k]]));
          const map = new Map();
          for (const top of plan.tops) {
            const P = top.P || M.I, Pi = M.inv(P) || M.I, flat = M.isI(P);
            const rec = (it, Wrel) => {
              if (it.kind === 'group') { const W2 = M.mul(Wrel, geo.matrix(it)); return Object.assign({}, it, { tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, children: (it.children || []).map(ch => rec(ch, W2)) }); }
              const out = byLeaf.get(it);
              if (!out) return M.pushDown(it, null, geo, Wrel, true);
              const o = asPath(ctx, it, flat ? out : M.transformSubpaths(out, Pi)); delete o.warp; return o;
            };
            map.set(top.it.id, rec(top.it, M.I));
          }
          ctx.replaceItems(map); ctx.select(plan.tops.map(t => t.it.id));
          ctx.commit(persp ? 'Perspective distort' : 'Free distort');
          log('designDistort', { mode: persp ? 'perspective' : 'free', parts: plan.leaves.length });
          st = null; sync(ctx); ctx.redraw();
        };
        if (plan.ops > HEAVY) { ctx.say('Distorting…', 0); runJobs(jobs, true).then(outs => { finish(outs); ctx.say(''); }); }
        else finish(jobs.map(runJob));
      },
      onKey(ctx, ev) { if (ev.key === 'Escape' && st && st.drag) { end(); st = null; sync(ctx); ctx.redraw(); return true; } return false; },
      overlay(ctx) {
        sync(ctx);
        if (!st || !st.quad) return '';
        const q = st.quad.map(c => ctx.stageOf(c[0], c[1]));
        let h = `<polygon class="cw-quad" points="${q.map(c => c[0].toFixed(1) + ',' + c[1].toFixed(1)).join(' ')}"/>`;
        ['nw', 'ne', 'se', 'sw'].forEach((n, k) => { h += `<rect class="u-h" data-h="${n}" x="${(q[k][0] - 4.5).toFixed(1)}" y="${(q[k][1] - 4.5).toFixed(1)}" width="9" height="9"/>`; });
        return h;
      }
    };
  }

  /* ═══ 5d · registering (CharmEditor's extension points; before the editor loads, its stand-in object is taken in) ═══ */
  function register() {
    const CE = root.CharmEditor || (root.CharmEditor = {});
    const tools = CE.tools || (CE.tools = []), commands = CE.commands || (CE.commands = {}), onClose = CE.onClose || (CE.onClose = []);
    if (tools.some(t => t && t.id === 'cw-blend')) return;
    tools.push(blendTool, distortTool('cw-free', false), distortTool('cw-persp', true));
    const sel = ctx => ctx.selection().some(id => { const e = ctx.entry(id); return e && !e.it.locked; });
    Object.assign(commands, {
      'cw-blend-make': { title: 'Make blend…', key: 'Mod+Alt+B', group: 'Blend', icon: IC.blend, enabled: canBlend, run: ctx => openBlend(ctx) },
      'cw-blend-options': { title: 'Blend options…', group: 'Blend', enabled: ctx => !!selBlend(ctx), run: ctx => openBlend(ctx) },
      'cw-blend-spine': { title: 'Replace spine', group: 'Blend', enabled: ctx => { const ids = ctx.selection(); if (ids.length !== 2) return false; const es = ids.map(id => ctx.entry(id)); return es.some(e => e && e.it.blend) && es.some(e => e && isOpenPath(e.it)); }, run: ctx => openBlend(ctx) },
      'cw-blend-release': { title: 'Release blend', key: 'Mod+Alt+Shift+B', group: 'Blend', enabled: ctx => !!selBlend(ctx), run: releaseBlend },
      'cw-blend-expand': { title: 'Expand blend', group: 'Blend', enabled: ctx => !!selBlend(ctx), run: expandBlend },
      'cw-warp': { title: 'Warp…', key: 'Mod+Alt+Shift+W', group: 'Warp & distort', icon: IC.warp, enabled: sel, run: openWarp },
      'cw-warp-expand': { title: 'Expand warp', group: 'Warp & distort', enabled: hasWarp, run: expandWarp },
      'cw-free': { title: 'Free distort', group: 'Warp & distort', icon: IC.distort, enabled: sel, run: ctx => ctx.setTool('cw-free') },
      'cw-persp': { title: 'Perspective distort', group: 'Warp & distort', icon: IC.perspective, enabled: sel, run: ctx => ctx.setTool('cw-persp') }
    });
    onClose.push(() => { if (dlg) dlg.cancel(); });
  }
  register();
  Object.assign(api, { openWarp, openBlend, register, stats });
  return api;
});
