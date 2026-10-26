/* charm-nest-overlap-fix.js — OVERLAPFIX (Paul, 25 Oct 2026): "minor overlap between some charms on a sheet … must be auto fixed by the system by shrinking a given charm that is
 * overlapping by the smallest possible amount and moving it over just slightly enough that the offending overlap gets resolved … minimal effort, minimal movement and minimal resizing of
 * charms only if necessary. The primary approach should be that a tiny readjustment of the position should resolve the tiny overlap, but if that is not possible, then shrinking the charm
 * by the smallest needed amount so that the overlap disappears and the charm can be repositioned very slightly."
 *
 * What this file is. The PLANNER, pure (no DOM, no pdf.js): the render verifier's own pixels in (charm-nest-pdf.js verifyRendered: per charm layer an area { X0, Y0, bw, bh, core }, core = 1
 * where the charm's material counts, already eroded by the allowed stroke share, in page pixels at `res` px/pt), a list of nudges out. The caller applies them, writes the file again and
 * verifies it again: this only plans.
 *
 *   plan({ W, H, inset, res, areas, centers, movable, prefer, maxShiftPx = 3·res, maxShrink = 0.06, shrinkStep = 0.005, marginPx = 1 })
 *     -> { ok, fixes: [{ name, dx, dy, scale, movedPt, shrinkPct, marginPx }], unresolved: [{ a, b, px, pair }], why }
 *   areas    Map name -> area (an { empty } area is ignored)        centers  Map name -> { x, y } the charm's pivot in page px (y down; pixel X spans [X, X+1)); shrinking is about it
 *   movable  Set of names the plan may touch (omitted: all)          prefer   Map name -> number, lower is touched first (omitted: equal)
 *   a fix: scale (≤ 1) about the centre first, then (dx, dy) whole page pixels.
 *
 * How. All overlapping pairs are found from the cores (box rejection, then the shared pixels counted), largest overlap first. For a pair the charm to touch is a movable one: the lowest
 * `prefer`, then the smaller core. Its core, grown by marginPx (a re-render's antialiasing must not bring the overlap back), is tried at every whole-pixel shift within maxShiftPx, nearest
 * first (rings of equal Euclidean distance; within a ring the direction away from its overlap first), and the first shift where it touches no other charm's core (every other charm, those
 * already fixed at their new places included) and its core stays inside the inset wins: a shift away from the overlap is naturally the nearest that clears it. Only when no shift of either
 * charm of the pair clears it, the core is shrunk about its centre in steps of shrinkStep (nearest-neighbour: a planner; the caller renders the real thing and checks it) down to
 * 1 - maxShrink, each scale with the same nearest-shift search; the smallest shrink that works wins, with its smallest shift. When nothing clears it with the margin, the same search
 * runs once more without it (the cores still never share a pixel; the caller's check after the re-render decides). A fixed charm touches nothing any more, so every overlap it had is
 * gone with it and no fix ever makes a new one. Each test reads an occupancy grid of the other charms (Int16 counts per page pixel) at the moved mask's pixels only, its outline first. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.CharmNestOverlapFix = api;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const asMap = m => m instanceof Map ? m : new Map(m ? Object.entries(m) : []);
  const r2 = (v, d) => +v.toFixed(d);

  /** The area scaled by s about (cx, cy) (page px), nearest neighbour; s ≥ 1 returns the area itself. */
  function scaleArea(area, center, s) {
    if (!(s < 1)) return area;
    const { X0, Y0, bw, bh, core } = area;
    const cx = center ? center.x : X0 + bw / 2, cy = center ? center.y : Y0 + bh / 2;
    const nx0 = Math.floor(cx + s * (X0 - cx)), nx1 = Math.ceil(cx + s * (X0 + bw - cx));
    const ny0 = Math.floor(cy + s * (Y0 - cy)), ny1 = Math.ceil(cy + s * (Y0 + bh - cy));
    const nw = Math.max(0, nx1 - nx0), nh = Math.max(0, ny1 - ny0);
    const sx = new Int32Array(nw), sy = new Int32Array(nh), out = new Uint8Array(nw * nh);
    for (let x = 0; x < nw; x++) { const u = Math.floor(cx + (nx0 + x + 0.5 - cx) / s) - X0; sx[x] = u >= 0 && u < bw ? u : -1; }
    for (let y = 0; y < nh; y++) { const v = Math.floor(cy + (ny0 + y + 0.5 - cy) / s) - Y0; sy[y] = v >= 0 && v < bh ? v : -1; }
    for (let y = 0; y < nh; y++) {
      if (sy[y] < 0) continue;
      const src = sy[y] * bw, dst = y * nw;
      for (let x = 0; x < nw; x++) if (sx[x] >= 0 && core[src + sx[x]]) out[dst + x] = 1;
    }
    return { X0: nx0, Y0: ny0, bw: nw, bh: nh, core: out };
  }

  /** The area a fix leaves: scaled about the centre, then shifted by whole pixels. */
  function transformArea(area, center, fix) {
    const a = scaleArea(area, center, fix && fix.scale);
    return { X0: a.X0 + ((fix && fix.dx) | 0), Y0: a.Y0 + ((fix && fix.dy) | 0), bw: a.bw, bh: a.bh, core: a.core };
  }

  /** Every pair of areas whose cores share a pixel: [{ a, b, px }] (a before b in the map's order), largest first. */
  function findOverlaps(areas) {
    const list = [...asMap(areas)].filter(([, a]) => a && !a.empty && a.core);
    const out = [];
    for (let i = 0; i < list.length; i++) for (let j = i + 1; j < list.length; j++) {
      const px = sharedPx(list[i][1], list[j][1]);
      if (px) out.push({ a: list[i][0], b: list[j][0], px, i, j });
    }
    out.sort((p, q) => q.px - p.px || p.i - q.i || p.j - q.j);
    return out.map(({ a, b, px }) => ({ a, b, px }));
  }
  function sharedPx(A, B) {
    const x0 = Math.max(A.X0, B.X0), x1 = Math.min(A.X0 + A.bw, B.X0 + B.bw), y0 = Math.max(A.Y0, B.Y0), y1 = Math.min(A.Y0 + A.bh, B.Y0 + B.bh);
    if (x0 >= x1 || y0 >= y1) return 0;
    let n = 0;
    for (let Y = y0; Y < y1; Y++) {
      const ra = (Y - A.Y0) * A.bw - A.X0, rb = (Y - B.Y0) * B.bw - B.X0;
      for (let X = x0; X < x1; X++) if (A.core[ra + X] && B.core[rb + X]) n++;
    }
    return n;
  }

  /* One mask ready for the search: its core pixels (page coordinates, for the grid and the inset test) and the core grown by m, outline first (nearest the overlap first: a blocked
     shift is usually rejected by its first few pixels) and then the inside. */
  function prep(area, m, fx, fy) {
    const { X0, Y0, bw, bh, core } = area;
    let n = 0;
    for (let b = 0; b < core.length; b++) if (core[b]) n++;
    const xs = new Int32Array(n), ys = new Int32Array(n);
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (let y = 0, b = 0, k = 0; y < bh; y++) for (let x = 0; x < bw; x++, b++) {
      if (!core[b]) continue;
      const X = X0 + x, Y = Y0 + y; xs[k] = X; ys[k] = Y; k++;
      if (X < minX) minX = X; if (X > maxX) maxX = X; if (Y < minY) minY = Y; if (Y > maxY) maxY = Y;
    }
    const dw = bw + 2 * m, dh = bh + 2 * m, dil = new Uint8Array(dw * dh);
    for (let y = 0, b = 0; y < bh; y++) for (let x = 0; x < bw; x++, b++) {
      if (!core[b]) continue;
      for (let yy = 0; yy <= 2 * m; yy++) { const row = (y + yy) * dw + x; for (let xx = 0; xx <= 2 * m; xx++) dil[row + xx] = 1; }
    }
    const edge = [], inner = [];
    for (let y = 0, b = 0; y < dh; y++) for (let x = 0; x < dw; x++, b++) {
      if (!dil[b]) continue;
      const out = x === 0 || y === 0 || x === dw - 1 || y === dh - 1 || !dil[b - 1] || !dil[b + 1] || !dil[b - dw] || !dil[b + dw];
      (out ? edge : inner).push(X0 - m + x, Y0 - m + y);
    }
    const order = [];
    for (let i = 0; i < edge.length; i += 2) order.push(i);
    order.sort((i, j) => ((edge[i] - fx) ** 2 + (edge[i + 1] - fy) ** 2) - ((edge[j] - fx) ** 2 + (edge[j + 1] - fy) ** 2) || i - j);
    const tx = new Int32Array(edge.length / 2 + inner.length / 2), ty = new Int32Array(tx.length);
    let k = 0;
    for (const i of order) { tx[k] = edge[i]; ty[k] = edge[i + 1]; k++; }
    for (let i = 0; i < inner.length; i += 2) { tx[k] = inner[i]; ty[k] = inner[i + 1]; k++; }
    return { n, xs, ys, minX, maxX, minY, maxY, tx, ty };
  }

  function plan(opt) {
    const o = opt || {};
    const W = o.W | 0, H = o.H | 0, inset = Math.max(0, o.inset | 0), res = o.res > 0 ? o.res : 6;
    const maxShiftPx = Math.max(0, Math.floor(o.maxShiftPx != null ? o.maxShiftPx : 3 * res));
    const maxShrink = Math.max(0, Math.min(0.5, o.maxShrink != null ? +o.maxShrink : 0.06));
    const shrinkStep = o.shrinkStep > 0 ? +o.shrinkStep : 0.005;
    const marginPx = Math.max(0, o.marginPx != null ? o.marginPx | 0 : 1);
    const centers = asMap(o.centers), prefer = asMap(o.prefer);
    const movable = o.movable == null ? null : new Set(o.movable);
    const list = [...asMap(o.areas)].filter(([, a]) => a && !a.empty && a.core && a.bw > 0 && a.bh > 0).map(([name, area], id) => ({ id, name, area }));
    if (!W || !H || !list.length) return { ok: true, fixes: [], unresolved: [], why: 'nothing to check' };

    const cnt = new Int16Array(W * H);
    const put = (P, dx, dy, d) => { for (let k = 0; k < P.n; k++) { const X = P.xs[k] + dx, Y = P.ys[k] + dy; if (X >= 0 && Y >= 0 && X < W && Y < H) cnt[Y * W + X] += d; } };
    for (const c of list) { c.P = prep(c.area, 0, 0, 0); c.px = c.P.n; c.cur = { P: c.P, dx: 0, dy: 0 }; put(c.P, 0, 0, 1); }

    const pairs = [];
    for (let i = 0; i < list.length; i++) for (let j = i + 1; j < list.length; j++) {
      const px = sharedPx(list[i].area, list[j].area);
      if (px) pairs.push({ i, j, px });
    }
    pairs.sort((p, q) => q.px - p.px || p.i - q.i || p.j - q.j);
    if (!pairs.length) return { ok: true, fixes: [], unresolved: [], why: 'no overlap' };

    // every whole-pixel shift within reach, by distance (the per-charm order adds the direction away from its overlap)
    const shifts = [];
    for (let dy = -maxShiftPx; dy <= maxShiftPx; dy++) for (let dx = -maxShiftPx; dx <= maxShiftPx; dx++) { const d2 = dx * dx + dy * dy; if (d2 <= maxShiftPx * maxShiftPx) shifts.push({ dx, dy, d2, dot: 0 }); }
    const scales = [];
    for (let k = 1; k * shrinkStep <= maxShrink + 1e-9; k++) scales.push(r2(1 - k * shrinkStep, 6));

    const fits = (P, dx, dy) => {
      if (!P.n || P.minX + dx < inset || P.maxX + dx >= W - inset || P.minY + dy < inset || P.maxY + dy >= H - inset) return false;
      const tx = P.tx, ty = P.ty;
      for (let k = 0; k < tx.length; k++) { const X = tx[k] + dx, Y = ty[k] + dy; if (X >= 0 && Y >= 0 && X < W && Y < H && cnt[Y * W + X]) return false; }
      return true;
    };
    const nearest = (P, order) => { for (const s of order) if (fits(P, s.dx, s.dy)) return s; return null; };

    /* One charm, one way (shift only, or shrink then shift) at one margin: the fix, or null. The charm is lifted off the grid while it is tried and put back where it ends. */
    function attempt(c, m, shrink) {
      const P0 = c.P;
      // where its overlaps are (pixels of its core another core also covers), for the direction away and the order of the outline test
      let ox = 0, oy = 0, on = 0, gx = 0, gy = 0;
      for (let k = 0; k < P0.n; k++) { const X = P0.xs[k], Y = P0.ys[k]; gx += X; gy += Y; if (X >= 0 && Y >= 0 && X < W && Y < H && cnt[Y * W + X] > 1) { ox += X; oy += Y; on++; } }
      gx /= P0.n || 1; gy /= P0.n || 1;
      const fx = on ? ox / on : gx, fy = on ? oy / on : gy;
      let ax = gx - fx, ay = gy - fy; const al = Math.hypot(ax, ay) || 1; ax /= al; ay /= al;
      const order = shifts.map(s => ({ dx: s.dx, dy: s.dy, d2: s.d2, dot: s.dx * ax + s.dy * ay }));
      order.sort((p, q) => p.d2 - q.d2 || q.dot - p.dot || p.dy - q.dy || p.dx - q.dx);
      put(P0, 0, 0, -1);
      let fix = null;
      if (!shrink) {
        const s = nearest(prep(c.area, m, fx, fy), order);
        if (s) fix = { scale: 1, dx: s.dx, dy: s.dy };
      } else {
        const center = centers.get(c.name) || null;
        let prev = null;
        for (const sc of scales) {
          const a = scaleArea(c.area, center, sc);
          if (prev && prev.X0 === a.X0 && prev.Y0 === a.Y0 && prev.bw === a.bw && prev.bh === a.bh && prev.core.every((v, i) => v === a.core[i])) continue;   // the same pixels: the same answer
          prev = a;
          const s = nearest(prep(a, m, fx, fy), order);
          if (s) { fix = { scale: sc, dx: s.dx, dy: s.dy, area: a }; break; }
        }
      }
      if (fix) { const P = fix.area ? prep(fix.area, 0, 0, 0) : P0; put(P, fix.dx, fix.dy, 1); }
      else put(P0, 0, 0, 1);
      return fix;
    }

    const prefOf = c => { const v = prefer.get(c.name); return typeof v === 'number' && isFinite(v) ? v : Infinity; };
    const canTouch = c => !movable || movable.has(c.name);
    const fixes = [], fixed = new Set(), failed = [];
    const margins = marginPx > 0 ? [marginPx, 0] : [0];
    for (const p of pairs) {
      const A = list[p.i], B = list[p.j];
      if (fixed.has(A.id) || fixed.has(B.id)) continue;   // a fixed charm touches nothing any more
      const cands = [A, B].filter(canTouch).sort((x, y) => prefOf(x) - prefOf(y) || x.px - y.px || x.id - y.id);
      if (!cands.length) { failed.push({ p, why: 'neither charm may be moved' }); continue; }
      let got = null, who = null, mUsed = 0;
      search: for (const m of margins) for (const shrink of [false, true]) for (const c of cands) {
        const f = attempt(c, m, shrink);
        if (f) { got = f; who = c; mUsed = m; break search; }
      }
      if (!got) { failed.push({ p, why: cands.length === 1 ? `${cands[0].name} cannot be cleared within the limits (the other is not movable)` : 'no shift or shrink within the limits clears either charm' }); continue; }
      fixed.add(who.id);
      fixes.push({ name: who.name, dx: got.dx, dy: got.dy, scale: got.scale, movedPt: r2(Math.hypot(got.dx, got.dy) / res, 2), shrinkPct: r2((1 - got.scale) * 100, 2), marginPx: mUsed });
    }
    const left = failed.filter(f => !fixed.has(list[f.p.i].id) && !fixed.has(list[f.p.j].id));
    const unresolved = left.map(f => ({ a: list[f.p.i].name, b: list[f.p.j].name, px: f.p.px, pair: list[f.p.i].name + ' ↔ ' + list[f.p.j].name, why: f.why }));
    const said = fixes.map(f => `${f.name} ${f.scale < 1 ? `${f.shrinkPct} % smaller, ` : ''}moved ${f.movedPt} pt`);
    const why = (fixes.length ? `${fixes.length} charm${fixes.length === 1 ? '' : 's'} adjusted: ${said.join('; ')}` : 'nothing adjusted')
      + (unresolved.length ? `; unresolved: ${unresolved.map(u => `${u.pair} (${u.why})`).join('; ')}` : '');
    return { ok: unresolved.length === 0, fixes, unresolved, why };
  }

  return { plan, scaleArea, transformArea, findOverlaps };
});
