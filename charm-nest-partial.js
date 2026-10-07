/* Partial sheets: how many regular pieces can reasonably fit on a leftover (Paul, 7 Oct 2026: "an estimate of how many regular pieces can
 * reasonably fit on the remaining partial sheet"). ONE pure function for the page, the workers and the server (UMD, as charm-nest-rose.js):
 *
 *     CharmNestPartial.estimateFit(rings, typical, opts) -> { pieces, low, high, packedPct, usableMm2, packMm2 }
 *
 * THIS IS AN ESTIMATE. The nester decides the real fit, piece by piece. The number is a conservative guide for a card and for "do all my
 * current pieces fit here", never a promise.
 *
 * Input
 *   rings    the leftover's exact outline: closed rings [[x,y],...] in real mm, origin top left, even-odd fill (Charm_Nest_Remnants `rings`).
 *   typical  the metal's regular piece: { areaMm2, minMm, maxMm }
 *              areaMm2  the average footprint of ONE piece INCLUDING its spacing: the piece's silhouette grown by half the clearance on every
 *                       side (the page's inflatedArea). It is NOT divided by the packing density: the density below does that once.
 *              minMm / maxMm  the average shorter / longer side of a piece's box (how wide a strip must be to take one).
 *            (a number is read as areaMm2 alone; wMm / hMm are read as minMm / maxMm). Missing parts come from DEFAULT_TYPICAL.
 *   opts     sheetWMm, sheetHMm (the whole physical sheet; default: the outline's far edges), insetMm (the sheet's inset band, default 0.53 mm
 *            = the page's 1.5 pt), edgeMm (the least distance kept from a green cut edge, default 1 mm = charm-nest-rose.js EDGE_PT),
 *            density (default 0.75), lowDensity (0.66), highDensity (0.80).
 *
 * Formula
 *   1. usable area U  = the part of the outline that is at least insetMm from the sheet's own border AND at least edgeMm from any green cut edge
 *                       (worked out on a grid of at most about 200 cells across, with a distance transform: cheap, no clipping library).
 *   2. thin strips    O(r) = the part of U that a disc of radius r can still reach while staying inside U (the "opening" of U), with
 *                       r = typical.minMm / 2: a strip narrower than a typical piece, and a corner that cannot hold one, count for nothing.
 *   3. pieces         = floor( density * O(r) / typical.areaMm2 )          density 0.75: the sorter's target on a full sheet (75 percent, toward 80). A leftover is
 *                                                                            packed as one tight block from the left, so it is close to a clean rectangle: no extra pessimism.
 *      low            = floor( lowDensity  * O(r)   / typical.areaMm2 )     0.66, the careful end
 *      high           = floor( highDensity * O(r/2) / typical.areaMm2 )     0.80 (the sorter's ceiling) and strips half as wide still count: smaller pieces fit them
 *   4. packedPct      = round( 100 * pieces * typical.areaMm2 / U )         the share of the usable area those pieces would fill: about 72 to 75 for a clean
 *                                                                            block (the floor takes a little), lower where thin strips are wasted.
 *   The default regular piece (DEFAULT_TYPICAL) is a real 14K Gold charm: 61 mm2 including its spacing (6.7 x 10.4 mm), from a live sheet 50 x 46 mm that held 13
 *   pieces at 36 percent full. (Paul, 7 Oct 2026: a clean 50 x 44.4 mm sheet "should fit between 24 to 28 parts".)
 *   Example 1: a clean new sheet 50 x 44.4 mm (the whole sheet is the outline; only the 0.53 mm inset band is lost): U = (50 - 1.06) x (44.4 - 1.06) = about 2,120 mm2,
 *   O = about 2,108 mm2 (the four corners are a little too tight for a whole piece); pieces = floor(0.75 x 2,108 / 61) = 25; low = floor(0.66 x 2,108 / 61) = 22;
 *   high = floor(0.80 x 2,115 / 61) = 27.
 *   Example 2: a clean 50 x 40 mm block in the top left corner of a 100 x 50 mm sheet (green cut edge on its right and bottom, sheet border on the other two):
 *   U = (50 - 0.53 - 1) x (40 - 0.53 - 1) = about 1,870 mm2; no thin part, so O = about 1,860; pieces = floor(0.75 x 1,860 / 61) = 22; low = floor(0.66 x 1,860 / 61) = 20;
 *   high = floor(0.80 x 1,870 / 61) = 24; packedPct = round(100 x 22 x 61 / 1,870) = 72.
 *   A 100 x 6 mm strip holds none of a typical (6.7 mm wide) piece: pieces 0, high 5 (smaller pieces). A 100 x 12 mm strip: 12 (11 to 13).

 *     CharmNestPartial.planFor(cards, pieces, opts) -> { fitsAll, needed, partials, needMm2, haveMm2, short, estimate:true }
 *   Which of the (available) partial cards would be needed for these pieces, filled one after the other in `opts.order` ('recent' = newest used first,
 *   the list's own order, the default; 'largest'; 'smallest'), and whether they all fit. Same estimate, per card: each card takes up to
 *   `packMm2` (density * O(r)) of the pieces' footprint; the pieces' own average replaces the metal's typical one. Unlimited cards.
 *   pieces: [{ areaMm2, wMm?, hMm? }, ...]  or  { areaMm2 (the total), count }  or  a total area number.
 */
(function (root, factory) { const api = factory(); if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestPartial = api; })(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const MM = 25.4 / 72;
  // used when a metal has no history yet, and as a gentle prior (PRIOR pieces' worth) under the metal's own average
  const DEFAULT_TYPICAL = { areaMm2: 61, minMm: 6.7, maxMm: 10.4 }, PRIOR = 5;
  const DEFAULTS = { insetMm: 1.5 * MM, edgeMm: 1, density: 0.75, lowDensity: 0.66, highDensity: 0.80 };
  const num = (v, d) => (Number.isFinite(+v) && +v > 0 ? +v : d);

  // typical -> { areaMm2, minMm, maxMm }, every part positive
  function typicalOf(t) {
    if (Number.isFinite(+t) && +t > 0) t = { areaMm2: +t };
    t = t || {};
    const areaMm2 = num(t.areaMm2, DEFAULT_TYPICAL.areaMm2);
    // a piece of another size than the default keeps the default's shape (sides scale with the square root of the area) unless its sides are known
    const k = Math.sqrt(areaMm2 / DEFAULT_TYPICAL.areaMm2);
    const minMm = num(t.minMm, num(t.wMm && t.hMm ? Math.min(t.wMm, t.hMm) : 0, DEFAULT_TYPICAL.minMm * k));
    const maxMm = Math.max(minMm, num(t.maxMm, num(t.wMm && t.hMm ? Math.max(t.wMm, t.hMm) : 0, DEFAULT_TYPICAL.maxMm * k)));
    return { areaMm2, minMm, maxMm };
  }
  /* The metal's typical piece from its running sums (what each cut adds: charm-nest-partial stats doc { n, areaMm2, minMm, maxMm } sums), blended with
     the default as PRIOR pieces so the first cuts cannot make it silly. `sums` null/empty = the default. */
  function typicalFromSums(sums) {
    const n = sums && +sums.n > 0 ? +sums.n : 0;
    if (!n) return { ...DEFAULT_TYPICAL, n: 0 };
    const mix = (s, d) => ((+s || 0) + PRIOR * d) / (n + PRIOR);
    return { areaMm2: +mix(sums.areaMm2, DEFAULT_TYPICAL.areaMm2).toFixed(2), minMm: +mix(sums.minMm, DEFAULT_TYPICAL.minMm).toFixed(2), maxMm: +mix(sums.maxMm, DEFAULT_TYPICAL.maxMm).toFixed(2), n };
  }

  // distance (in cells) from every cell to the nearest source cell (mask[i] = 1): the two-pass chamfer transform (1 and sqrt 2)
  function distance(mask, nx, ny) {
    const D = new Float32Array(nx * ny), R2 = Math.SQRT2, INF = 1e9;
    for (let i = 0; i < D.length; i++) D[i] = mask[i] ? 0 : INF;
    for (let y = 0; y < ny; y++) for (let x = 0; x < nx; x++) {
      const i = y * nx + x; let d = D[i];
      if (x > 0 && D[i - 1] + 1 < d) d = D[i - 1] + 1;
      if (y > 0) { if (D[i - nx] + 1 < d) d = D[i - nx] + 1; if (x > 0 && D[i - nx - 1] + R2 < d) d = D[i - nx - 1] + R2; if (x < nx - 1 && D[i - nx + 1] + R2 < d) d = D[i - nx + 1] + R2; }
      D[i] = d;
    }
    for (let y = ny - 1; y >= 0; y--) for (let x = nx - 1; x >= 0; x--) {
      const i = y * nx + x; let d = D[i];
      if (x < nx - 1 && D[i + 1] + 1 < d) d = D[i + 1] + 1;
      if (y < ny - 1) { if (D[i + nx] + 1 < d) d = D[i + nx] + 1; if (x < nx - 1 && D[i + nx + 1] + R2 < d) d = D[i + nx + 1] + R2; if (x > 0 && D[i + nx - 1] + R2 < d) d = D[i + nx - 1] + R2; }
      D[i] = d;
    }
    return D;
  }

  // the cells whose centre is inside the rings (even-odd), one row at a time
  function fill(rings, nx, ny, c) {
    const inside = new Uint8Array(nx * ny), edges = [];
    for (const ring of rings || []) for (let a = 0, b = ring.length - 1; a < ring.length; b = a++) { const p = ring[b], q = ring[a]; if (p[1] !== q[1]) edges.push(p[1] < q[1] ? [p[0], p[1], q[0], q[1]] : [q[0], q[1], p[0], p[1]]); }
    for (let y = 0; y < ny; y++) {
      const yc = (y + 0.5) * c, xs = [];
      for (const [x0, y0, x1, y1] of edges) if (yc >= y0 && yc < y1) xs.push(x0 + (yc - y0) * (x1 - x0) / (y1 - y0));
      xs.sort((a, b) => a - b);
      for (let k = 0; k + 1 < xs.length; k += 2) {
        const from = Math.max(0, Math.ceil(xs[k] / c - 0.5)), to = Math.min(nx - 1, Math.ceil(xs[k + 1] / c - 0.5) - 1);
        for (let x = from; x <= to; x++) inside[y * nx + x] = 1;
      }
    }
    return inside;
  }

  function estimateFit(rings, typical, opts) {
    opts = opts || {};
    const t = typicalOf(typical), insetMm = Math.max(0, num(opts.insetMm, DEFAULTS.insetMm)), edgeMm = Math.max(0, num(opts.edgeMm, DEFAULTS.edgeMm));
    const dens = num(opts.density, DEFAULTS.density), lowDens = num(opts.lowDensity, DEFAULTS.lowDensity), highDens = num(opts.highDensity, DEFAULTS.highDensity);
    const zero = { pieces: 0, low: 0, high: 0, packedPct: 0, usableMm2: 0, packMm2: 0 };
    let maxX = 0, maxY = 0;
    for (const ring of rings || []) for (const p of ring) { if (p[0] > maxX) maxX = p[0]; if (p[1] > maxY) maxY = p[1]; }
    const W = num(opts.sheetWMm, maxX), H = num(opts.sheetHMm, maxY);
    if (!(W > 0 && H > 0) || !(rings && rings.length)) return zero;
    const c = Math.min(2.5, Math.max(0.25, Math.max(W, H) / 200)), nx = Math.max(1, Math.round(W / c)), ny = Math.max(1, Math.round(H / c)), n = nx * ny;
    const inside = fill(rings, nx, ny, c);
    // blocked by a CUT: a cell of the sheet that is not in the outline (distance to it, less half a cell, is the distance to the cut edge)
    const cutSrc = new Uint8Array(n); let any = 0;
    for (let i = 0; i < n; i++) { if (inside[i]) any++; else cutSrc[i] = 1; }
    if (!any) return zero;
    const dCut = any === n ? null : distance(cutSrc, nx, ny);
    const usable = new Uint8Array(n); let U = 0;
    for (let y = 0; y < ny; y++) for (let x = 0; x < nx; x++) {
      const i = y * nx + x; if (!inside[i]) continue;
      const cx = (x + 0.5) * c, cy = (y + 0.5) * c, border = Math.min(cx, W - cx, cy, H - cy);
      if (border < insetMm - 1e-9) continue;
      if (dCut && dCut[i] * c - c / 2 < edgeMm - 1e-9) continue;
      usable[i] = 1; U++;
    }
    if (!U) return zero;
    // the opening of the usable part by a disc of radius r: erode (distance to what is not usable >= r), then grow back (within r of what is left)
    const unusable = new Uint8Array(n); for (let i = 0; i < n; i++) unusable[i] = usable[i] ? 0 : 1;
    const dOut = distance(unusable, nx, ny);   // (the sheet's own outside is "not usable" too: the grid ends at the sheet's edge, so a distance of half a cell to it is added below)
    const opened = r => {
      const core = new Uint8Array(n); let any2 = 0;
      for (let y = 0; y < ny; y++) for (let x = 0; x < nx; x++) {
        const i = y * nx + x; if (!usable[i]) continue;
        // distance from this cell's centre to the nearest unusable cell or to the grid's end (the sheet's edge is unusable too)
        const edge = Math.min(x + 0.5, nx - x - 0.5, y + 0.5, ny - y - 0.5);
        if (Math.min(dOut[i] - 0.5, edge) * c >= r - c / 2 - 1e-9) { core[i] = 1; any2++; }
      }
      if (!any2) return 0;
      const dCore = distance(core, nx, ny); let count = 0;
      for (let i = 0; i < n; i++) if (usable[i] && dCore[i] * c <= r + c / 2 + 1e-9) count++;
      return count * c * c;
    };
    const usableMm2 = U * c * c, rBase = t.minMm / 2;
    const O1 = Math.min(usableMm2, opened(rBase)), O2 = Math.min(usableMm2, opened(rBase / 2));
    const pieces = Math.floor(dens * O1 / t.areaMm2 + 1e-9);
    const low = Math.min(pieces, Math.floor(lowDens * O1 / t.areaMm2 + 1e-9));
    const high = Math.max(pieces, Math.floor(highDens * O2 / t.areaMm2 + 1e-9));
    return { pieces, low, high, packedPct: Math.min(100, Math.round(100 * pieces * t.areaMm2 / usableMm2)), usableMm2: +usableMm2.toFixed(1), packMm2: +(dens * O1).toFixed(1) };
  }

  /* What the pieces need and how partial cards would take them. cards: [{ id, outline, sheetWMm, sheetHMm, lastUsedAt, ... }] */
  function planFor(cards, pieces, opts) {
    opts = opts || {};
    const base = typicalOf(opts.typical);
    let need = 0, count = 0, area = 0, minMm = 0, maxMm = 0;
    if (Array.isArray(pieces)) {
      const list = pieces.map(p => (typeof p === 'number' ? { areaMm2: p } : p || {})).filter(p => num(p.areaMm2, 0) > 0);
      count = list.length; need = list.reduce((s, p) => s + p.areaMm2, 0); area = count ? need / count : 0;
      const sides = list.filter(p => num(p.wMm, 0) && num(p.hMm, 0));
      if (sides.length) { minMm = sides.reduce((s, p) => s + Math.min(p.wMm, p.hMm), 0) / sides.length; maxMm = sides.reduce((s, p) => s + Math.max(p.wMm, p.hMm), 0) / sides.length; }
    } else if (pieces && typeof pieces === 'object') {
      need = num(pieces.areaMm2, 0); count = Math.max(0, Math.floor(+pieces.count) || 0); area = count && need ? need / count : 0; minMm = num(pieces.minMm, 0); maxMm = num(pieces.maxMm, 0);
    } else need = num(pieces, 0);
    // the pieces' own average stands in for the metal's typical one where it is known; sides not known follow the metal's shape at the new size
    const k = area ? Math.sqrt(area / base.areaMm2) : 1;
    const typical = typicalOf({ areaMm2: area || base.areaMm2, minMm: minMm || base.minMm * k, maxMm: maxMm || base.maxMm * k });
    if (!count && need) count = Math.ceil(need / typical.areaMm2 - 1e-9);
    const order = ['largest', 'smallest'].includes(opts.order) ? opts.order : 'recent';
    const rows = (cards || []).filter(c => c && c.outline && (!c.status || c.status === 'available' || opts.anyStatus)).map((c, i) => {
      const e = estimateFit(c.outline, typical, { sheetWMm: c.sheetWMm, sheetHMm: c.sheetHMm, ...(opts.fit || {}) });
      return { id: c.id, i, at: +c.lastUsedAt || +c.cutAt || 0, capacityMm2: e.packMm2, pieces: e.pieces, usableMm2: e.usableMm2 };
    });
    if (order === 'recent') rows.sort((a, b) => b.at - a.at || a.i - b.i);
    else rows.sort((a, b) => (order === 'largest' ? b.capacityMm2 - a.capacityMm2 : a.capacityMm2 - b.capacityMm2) || a.i - b.i);
    let left = need, left_n = count; const partials = [];
    for (const r of rows) {
      if (left <= 1e-6) break;
      if (!(r.capacityMm2 > 0)) continue;
      const usedMm2 = Math.min(r.capacityMm2, left), take = Math.min(left_n, Math.max(1, Math.min(r.pieces, Math.round(usedMm2 / typical.areaMm2))));
      partials.push({ id: r.id, capacityMm2: r.capacityMm2, pieces: take, usedMm2: +usedMm2.toFixed(1) });
      left -= usedMm2; left_n = Math.max(0, left_n - take);
    }
    const haveMm2 = +rows.reduce((s, r) => s + r.capacityMm2, 0).toFixed(1), fitsAll = need > 0 && left <= 1e-6;
    return { ok: true, fitsAll: need > 0 ? fitsAll : true, needed: partials.map(p => p.id), partials, needMm2: +need.toFixed(1), haveMm2,
      short: { mm2: +Math.max(0, left).toFixed(1), pieces: left > 1e-6 ? Math.max(1, left_n || Math.ceil(left / typical.areaMm2)) : 0 }, typical, estimate: true };
  }

  return { estimateFit, planFor, typicalOf, typicalFromSums, DEFAULT_TYPICAL, DEFAULTS, PRIOR, MM };
});
