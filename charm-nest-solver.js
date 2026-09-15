/*  charm-nest-solver.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  Raster nesting solver for the Charm Nesting Station.
 *
 *  One file, three hosts: the browser Web Worker (importScripts), Node
 *  (require, for the charmNestSolve-background fallback and the unit test)
 *  and, if ever needed, a plain <script>. No dependencies.
 *
 *  THE SEARCH
 *  Pieces are solid silhouettes (bitmaps), largest area first. For every
 *  piece and every allowed rotation the solver finds the legal positions on
 *  the sheet, scores each by how much of the piece's "contact ring" touches
 *  already-placed material or the sheet wall, breaks ties toward one corner
 *  (gravity), and commits the best. Restarts reshuffle the order, jitter the
 *  gravity weight and add score noise; the best layout is kept.
 *
 *  This is the contact-scored, multi-resolution search from the design
 *  document. The design document evaluated feasibility with FFT
 *  cross-correlation; this implementation evaluates the identical predicate
 *  with bit-parallel row tests instead:
 *
 *    · the sheet is a packed bitmap (32 cells per word);
 *    · every piece mask is pre-shifted into its 32 sub-word alignments once
 *      per angle, so a candidate position costs one AND per word with an
 *      early exit on the first collision;
 *    · a coarse pass (0.5 px/pt) scans every position exhaustively and keeps
 *      the best-scoring candidates; a fine pass (2 px/pt) tests the exact
 *      dilated masks in a small window around each candidate, so every
 *      committed placement is verified at full resolution — the coarse pass
 *      only decides where to look, never whether a placement is legal.
 *
 *  In JavaScript this runs a full 21-piece trial in about a second, so the
 *  three-minute default budget buys well over a hundred restarts instead of
 *  the dozen an FFT pass affords. Same predicate, same scoring, same
 *  guarantees; different arithmetic.
 *
 *  GUARANTEES
 *    · A committed placement never overlaps another piece or leaves the
 *      inset sheet area, at fine resolution, with each mask dilated by half
 *      the clearance — so any two silhouettes are at least `clearancePt`
 *      apart and every silhouette is at least `insetPt` from the sheet edge.
 *    · Mirroring is never applied. Engraved text must stay readable.
 *    · A piece that fits nowhere is rejected; the sheet continues.
 *    · The run is reproducible from (seed, angle set, clearance, resolution,
 *      piece order). The report carries all of them.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.CharmNestSolver = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  /* ── seeded RNG (mulberry32) ─────────────────────────────────────────── */
  function rng(seed) {
    let a = (seed >>> 0) || 1;
    return function () {
      a |= 0; a = (a + 0x6D2B79F5) | 0;
      let t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  function popcount32(v) {
    v = v - ((v >>> 1) & 0x55555555);
    v = (v & 0x33333333) + ((v >>> 2) & 0x33333333);
    return (((v + (v >>> 4)) & 0x0F0F0F0F) * 0x01010101) >>> 24;
  }

  /* ── bitmap helpers (Uint8Array, 1 byte per cell) ─────────────────────── */

  /** Rotate a 0/1 bitmap (w×h, y-down) by `deg` clockwise on screen about its
   *  centre. Supersampled 2×2 so thin features survive. Returns the tight
   *  bitmap plus the offset of the tight box from the rotation centre. */
  function rotateBitmap(bits, w, h, deg) {
    const th = deg * Math.PI / 180, c = Math.cos(th), s = Math.sin(th);
    const cx = w / 2, cy = h / 2;
    const W = Math.ceil(Math.abs(w * c) + Math.abs(h * s)) + 2;
    const H = Math.ceil(Math.abs(w * s) + Math.abs(h * c)) + 2;
    const CX = W / 2, CY = H / 2;
    const out = new Uint8Array(W * H);
    const sub = [0.25, 0.75];
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
      let hit = 0;
      for (const oy of sub) { for (const ox of sub) {
        const dx = x + ox - CX, dy = y + oy - CY;
        // inverse rotation
        const sx = c * dx + s * dy + cx, sy = -s * dx + c * dy + cy;
        const ix = sx | 0, iy = sy | 0;
        if (sx >= 0 && sy >= 0 && ix < w && iy < h && bits[iy * w + ix]) { hit = 1; break; }
      } if (hit) break; }
      if (hit) out[y * W + x] = 1;
    }
    return tight(out, W, H, CX, CY);
  }

  /** Crop to the tight bounding box; report centre offsets. */
  function tight(bits, W, H, CX, CY) {
    let x0 = W, y0 = H, x1 = -1, y1 = -1;
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) if (bits[y * W + x]) {
      if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y;
    }
    if (x1 < 0) return { bits: new Uint8Array(0), w: 0, h: 0, cx: 0, cy: 0 };
    const w = x1 - x0 + 1, h = y1 - y0 + 1, out = new Uint8Array(w * h);
    for (let y = 0; y < h; y++) out.set(bits.subarray((y + y0) * W + x0, (y + y0) * W + x0 + w), y * w);
    // centre of rotation relative to the tight box origin
    return { bits: out, w, h, cx: CX - x0, cy: CY - y0 };
  }

  /** Resample a bitmap from `sFrom` px/pt to `sTo` px/pt (any → set). */
  function resample(bits, w, h, sFrom, sTo) {
    const k = sFrom / sTo;
    const W = Math.max(1, Math.ceil(w / k)), H = Math.max(1, Math.ceil(h / k));
    const out = new Uint8Array(W * H);
    for (let y = 0; y < h; y++) {
      const ty = Math.min(H - 1, (y / k) | 0);
      for (let x = 0; x < w; x++) if (bits[y * w + x]) out[ty * W + Math.min(W - 1, (x / k) | 0)] = 1;
    }
    return { bits: out, w: W, h: H };
  }

  /** Disc dilation by r cells; the box grows by r on every side. */
  function dilate(bits, w, h, r) {
    if (r <= 0) return { bits: bits.slice(), w, h };
    const W = w + 2 * r, H = h + 2 * r, out = new Uint8Array(W * H);
    const offs = [];
    for (let dy = -r; dy <= r; dy++) for (let dx = -r; dx <= r; dx++) if (dx * dx + dy * dy <= r * r + r) offs.push([dx, dy]);
    // A disc dilation of a set is the set itself plus discs at its boundary
    // pixels, so interior pixels only copy themselves.
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) if (bits[y * w + x]) {
      const interior = x > 0 && y > 0 && x < w - 1 && y < h - 1 && bits[y * w + x - 1] && bits[y * w + x + 1] && bits[(y - 1) * w + x] && bits[(y + 1) * w + x];
      if (interior) { out[(y + r) * W + (x + r)] = 1; continue; }
      for (let i = 0; i < offs.length; i++) out[(y + r + offs[i][1]) * W + (x + r + offs[i][0])] = 1;
    }
    return { bits: out, w: W, h: H };
  }

  /** Ring = dilate(mask, r) minus mask. Same box as the dilation. */
  function ring(bits, w, h, r) {
    const d = dilate(bits, w, h, r);
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) if (bits[y * w + x]) d.bits[(y + r) * d.w + (x + r)] = 0;
    return d;
  }

  /** Disc erosion by r cells (the box shrinks by r on every side). */
  function erode(bits, w, h, r) {
    if (r <= 0) return { bits: bits.slice(), w, h };
    const W = Math.max(1, w - 2 * r), H = Math.max(1, h - 2 * r), out = new Uint8Array(W * H);
    const offs = [];
    for (let dy = -r; dy <= r; dy++) for (let dx = -r; dx <= r; dx++) if (dx * dx + dy * dy <= r * r + r) offs.push([dx, dy]);
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
      let ok = 1;
      for (let i = 0; i < offs.length; i++) { const sx = x + r + offs[i][0], sy = y + r + offs[i][1]; if (sx < 0 || sy < 0 || sx >= w || sy >= h || !bits[sy * w + sx]) { ok = 0; break; } }
      out[y * W + x] = ok;
    }
    return { bits: out, w: W, h: H };
  }
  function areaOf(bits) { let n = 0; for (let i = 0; i < bits.length; i++) n += bits[i]; return n; }

  /* ── packed masks: 32 pre-shifted copies ───────────────────────────────── */
  function packShifted(bits, w, h) {
    const words = ((w + 31) >> 5) + 1;                 // +1 so a shift never overflows
    const variants = new Array(32);
    for (let s = 0; s < 32; s++) {
      const arr = new Uint32Array(words * h);
      for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) if (bits[y * w + x]) {
        const px = x + s; arr[y * words + (px >> 5)] |= (1 << (px & 31)) >>> 0;
      }
      variants[s] = arr;
    }
    return { variants, words, w, h, cells: areaOf(bits) };
  }

  /* ── one resolution level of the sheet ─────────────────────────────────── */
  function Grid(W, H) {
    this.W = W; this.H = H; this.words = (W + 31) >> 5;
    this.occ = new Uint32Array(this.words * H);     // 1 = material or out of bounds
    this.free = new Uint8Array(W * H);               // 1 = free cell inside the sheet (for pocket search)
  }
  Grid.prototype.set = function (x, y) { this.occ[y * this.words + (x >> 5)] |= (1 << (x & 31)) >>> 0; this.free[y * this.W + x] = 0; };
  Grid.prototype.get = function (x, y) { return (this.occ[y * this.words + (x >> 5)] >>> (x & 31)) & 1; };
  Grid.prototype.clone = function () { const g = new Grid(this.W, this.H); g.occ.set(this.occ); g.free.set(this.free); if (this.sat) g.sat = this.sat.slice(); return g; };
  /** Legal position test: no overlap between the packed mask and the grid. */
  Grid.prototype.fits = function (pm, x, y) {
    if (x < 0 || y < 0 || x + pm.w > this.W || y + pm.h > this.H) return false;
    const v = pm.variants[x & 31], wx = x >> 5, gw = this.words, mw = pm.words, occ = this.occ;
    const wmax = Math.min(mw, gw - wx);
    for (let r = 0; r < pm.h; r++) {
      const gi = (y + r) * gw + wx, mi = r * mw;
      for (let k = 0; k < wmax; k++) if (occ[gi + k] & v[mi + k]) return false;
    }
    return true;
  };
  /** Count overlapping cells (used for coarse tolerance + contact scoring). */
  Grid.prototype.overlap = function (pm, x, y, limit) {
    const v = pm.variants[x & 31], wx = x >> 5, gw = this.words, mw = pm.words, occ = this.occ;
    let n = 0;
    for (let r = 0; r < pm.h; r++) {
      const gy = y + r; if (gy < 0 || gy >= this.H) { n += pm.cells; if (n > limit) return n; continue; }
      const gi = gy * gw + wx, mi = r * mw;
      for (let k = 0; k < mw; k++) {
        const gk = wx + k; const g = (gk < 0 || gk >= gw) ? 0xFFFFFFFF : occ[gi + k];
        const a = g & v[mi + k]; if (a) { n += popcount32(a); if (n > limit) return n; }
      }
    }
    return n;
  };
  Grid.prototype.stamp = function (bits, w, h, x, y) {
    for (let r = 0; r < h; r++) for (let c = 0; c < w; c++) if (bits[r * w + c]) this.set(x + c, y + r);
  };
  /** Summed-area table of occupancy; cells outside the grid count as occupied. */
  Grid.prototype.buildSAT = function () {
    const W = this.W, H = this.H, S = this.sat || (this.sat = new Int32Array((W + 1) * (H + 1)));
    for (let y = 1; y <= H; y++) { let row = 0; for (let x = 1; x <= W; x++) { row += this.get(x - 1, y - 1); S[y * (W + 1) + x] = S[(y - 1) * (W + 1) + x] + row; } }
    return S;
  };
  Grid.prototype.boxSum = function (x0, y0, x1, y1) {   // half-open [x0,x1) × [y0,y1)
    const W = this.W, H = this.H, S = this.sat;
    const cx0 = Math.max(0, x0), cy0 = Math.max(0, y0), cx1 = Math.min(W, x1), cy1 = Math.min(H, y1);
    let inside = 0;
    if (cx1 > cx0 && cy1 > cy0) inside = S[cy1 * (W + 1) + cx1] - S[cy0 * (W + 1) + cx1] - S[cy1 * (W + 1) + cx0] + S[cy0 * (W + 1) + cx0];
    const total = (x1 - x0) * (y1 - y0), clipped = Math.max(0, cx1 - cx0) * Math.max(0, cy1 - cy0);
    return inside + (total - clipped);                  // outside the grid is wall
  };
  Grid.prototype.freeCells = function () { let n = 0; const f = this.free; for (let i = 0; i < f.length; i++) n += f[i]; return n; };
  /** Largest axis-aligned empty rectangle (histogram/stack method). */
  Grid.prototype.largestPocket = function () {
    const W = this.W, H = this.H, f = this.free, hgt = new Int32Array(W);
    let best = { area: 0, x: 0, y: 0, w: 0, h: 0 };
    const stack = new Int32Array(W + 1);
    for (let y = 0; y < H; y++) {
      for (let x = 0; x < W; x++) hgt[x] = f[y * W + x] ? hgt[x] + 1 : 0;
      let sp = 0;
      for (let x = 0; x <= W; x++) {
        const cur = x < W ? hgt[x] : 0;
        while (sp > 0 && hgt[stack[sp - 1]] >= cur) {
          const hh = hgt[stack[--sp]];
          const left = sp > 0 ? stack[sp - 1] + 1 : 0;
          const ww = x - left, area = ww * hh;
          if (area > best.area) best = { area, x: left, y: y - hh + 1, w: ww, h: hh };
        }
        stack[sp++] = x;
      }
    }
    return best;
  };

  /* ── main solve ─────────────────────────────────────────────────────────── */

  /**
   * @param {object} job
   *   sheet        {wPt, hPt, insetPt}
   *   clearancePt  gap guaranteed between any two silhouettes
   *   angles       degrees, e.g. [0,30,...,330]  (never mirrored)
   *   fineRes      px/pt for the exact pass (default 2)
   *   coarseRes    px/pt for the candidate pass (default 0.5)
   *   timeBudgetMs total wall time (default 180 000)
   *   maxTrials    restart cap (default 400)
   *   seed         integer
   *   pieces       [{id, w, h, scale, bits(Uint8Array 0/1), areaPt2, pinned?:{cxPt,cyPt,angle}}]
   * @param {object} cb   optional callbacks:
   *   onStage(stage, done, total)  onPlaced(placement, trial)  onReject(id, trial)
   *   onTrial(summary)  onBest(layout)  shouldStop()  yield()  (async, lets a stop arrive)
   */
  async function solve(job, cb) {
    cb = cb || {};
    const t0 = now();
    const fineRes = job.fineRes || 2, coarseRes = job.coarseRes || 0.5;
    const ratio = Math.round(fineRes / coarseRes);
    const angles = (job.angles && job.angles.length ? job.angles : [0]).map(a => ((a % 360) + 360) % 360);
    // Negative clearance = allowed overlap: masks are ERODED by half of it, so two
    // outlines may touch and their strokes overlap by up to |clearance|, never more.
    const clearancePt = +job.clearancePt || 0;
    const insetPt = Math.max(0, job.sheet.insetPt == null ? 1.5 : +job.sheet.insetPt);
    const budget = job.timeBudgetMs || 180000;
    const maxTrials = job.maxTrials || 400;
    const random = rng(job.seed == null ? 1 : job.seed);
    const yieldNow = cb.yield || (() => new Promise(r => setTimeout(r, 0)));
    const stopped = () => (cb.shouldStop && cb.shouldStop()) || (now() - t0) > budget;

    /* sheet grids at both levels; the inset band is pre-filled as wall */
    const FW = Math.round(job.sheet.wPt * fineRes), FH = Math.round(job.sheet.hPt * fineRes);
    const CW = Math.ceil(FW / ratio), CH = Math.ceil(FH / ratio);
    const halfGapFine = clearancePt >= 0 ? Math.ceil(clearancePt / 2 * fineRes) : 0;
    const erodeFine = clearancePt < 0 ? Math.round(-clearancePt / 2 * fineRes) : 0;
    const wallFine = Math.round(insetPt * fineRes) + halfGapFine;
    const wallCoarse = Math.round(wallFine / ratio);
    const baseFine = new Grid(FW, FH), baseCoarse = new Grid(CW, CH);
    for (let y = 0; y < FH; y++) for (let x = 0; x < FW; x++) {
      if (x < wallFine || y < wallFine || x >= FW - wallFine || y >= FH - wallFine) baseFine.set(x, y); else baseFine.free[y * FW + x] = 1;
    }
    for (let y = 0; y < CH; y++) for (let x = 0; x < CW; x++) {
      if (x < wallCoarse || y < wallCoarse || x >= CW - wallCoarse || y >= CH - wallCoarse) baseCoarse.set(x, y); else baseCoarse.free[y * CW + x] = 1;
    }
    baseCoarse.buildSAT();
    const usableCellsFine = baseFine.freeCells();

    /* ── prepare every piece × angle once ───────────────────────────────── */
    const prepared = [];
    const pieces = job.pieces.slice();
    for (let i = 0; i < pieces.length; i++) {
      const p = pieces[i];
      if (stopped()) break;
      const fine = resample(p.bits, p.w, p.h, p.scale, fineRes);
      const solidFineCells = areaOf(fine.bits);
      const variants = [];
      const angleSet = p.pinned ? [((p.pinned.angle % 360) + 360) % 360] : angles;
      for (const a of angleSet) {
        const rot = rotateBitmap(fine.bits, fine.w, fine.h, a);
        if (!rot.w) continue;
        const dil = erodeFine ? erode(rot.bits, rot.w, rot.h, erodeFine) : dilate(rot.bits, rot.w, rot.h, halfGapFine);
        if (!dil.w || !areaOf(dil.bits)) continue;
        const rg = ring(dil.bits, dil.w, dil.h, Math.max(2, Math.round(2 * fineRes)));
        // coarse: majority resample of the dilated fine mask
        const co = majority(dil.bits, dil.w, dil.h, ratio);
        variants.push({
          angle: a,
          fine: { bits: dil.bits, w: dil.w, h: dil.h, pm: packShifted(dil.bits, dil.w, dil.h) },
          solid: { bits: rot.bits, w: rot.w, h: rot.h, cx: rot.cx + halfGapFine - erodeFine, cy: rot.cy + halfGapFine - erodeFine },
          ringFine: packShifted(rg.bits, rg.w, rg.h), ringPad: Math.max(2, Math.round(2 * fineRes)),
          coarse: { pm: packShifted(co.bits, co.w, co.h), w: co.w, h: co.h }
        });
      }
      prepared.push({ id: p.id, idx: i, areaPt2: p.areaPt2 || (solidFineCells / (fineRes * fineRes)), solidFineCells, variants, pinned: p.pinned || null, meta: p.meta || null });
      if (cb.onStage) cb.onStage("prepare", i + 1, pieces.length);
      await yieldNow();
    }

    /* ── trials ─────────────────────────────────────────────────────────── */
    let best = null, trials = 0, endedBy = "budget", failStreak = 0;
    const byAreaDesc = prepared.slice().sort((a, b) => b.areaPt2 - a.areaPt2);
    while (trials < maxTrials) {
      if (stopped()) { endedBy = cb.shouldStop && cb.shouldStop() ? "stopped" : "budget"; break; }
      const trial = trials++;
      // ordering: trial 0 = pure largest-first; later trials add noise growing with the streak
      const sigma = trial === 0 ? 0 : Math.min(0.6, 0.15 + 0.05 * Math.min(failStreak, 8));
      const order = byAreaDesc.map(p => ({ p, k: p.areaPt2 * (1 + sigma * (random() * 2 - 1)) })).sort((a, b) => b.k - a.k).map(o => o.p);
      const pinnedFirst = order.filter(p => p.pinned).concat(order.filter(p => !p.pinned));
      const gravW = trial === 0 ? 0.35 : 0.1 + random() * 0.8;
      const noise = trial === 0 ? 0 : random() * 0.15;
      const cornerX = trial === 0 ? 0 : (random() < 0.7 ? 0 : 1), cornerY = trial === 0 ? 0 : (random() < 0.7 ? 0 : 1);
      const fine = baseFine.clone(), coarse = baseCoarse.clone();
      const placements = [], rejects = [];
      let placedCells = 0;

      for (let pi = 0; pi < pinnedFirst.length; pi++) {
        const p = pinnedFirst[pi];
        if (stopped()) break;
        let bestPos = null;
        if (p.pinned) {
          const v = p.variants[0];
          const x = Math.round(p.pinned.cxPt * fineRes - v.solid.cx), y = Math.round(p.pinned.cyPt * fineRes - v.solid.cy);
          if (fine.fits(v.fine.pm, x, y)) bestPos = { v, x, y, score: Infinity };
        } else {
          bestPos = search(p, fine, coarse, ratio, gravW, noise, random, cornerX, cornerY);
        }
        if (!bestPos) {
          rejects.push(p.id);
          if (cb.onReject) cb.onReject(p.id, trial);
        } else {
          const { v, x, y } = bestPos;
          fine.stamp(v.fine.bits, v.fine.w, v.fine.h, x, y);
          const cx0 = Math.floor(x / ratio), cy0 = Math.floor(y / ratio);
          // coarse: mark every coarse cell touched by the fine mask (conservative)
          for (let r = 0; r < v.fine.h; r++) for (let c = 0; c < v.fine.w; c++) if (v.fine.bits[r * v.fine.w + c]) {
            const gx = Math.floor((x + c) / ratio), gy = Math.floor((y + r) / ratio);
            if (gx >= 0 && gy >= 0 && gx < coarse.W && gy < coarse.H) coarse.set(gx, gy);
          }
          void cx0; void cy0;
          coarse.buildSAT();
          placedCells += p.solidFineCells;
          const pl = {
            id: p.id, angle: v.angle,
            cxPt: (x + v.solid.cx) / fineRes, cyPt: (y + v.solid.cy) / fineRes,   // rotation centre, sheet pt, y-down
            xPt: (x + (v.fine.w - v.solid.w) / 2) / fineRes, yPt: (y + (v.fine.h - v.solid.h) / 2) / fineRes,
            wPt: v.solid.w / fineRes, hPt: v.solid.h / fineRes,
            contact: bestPos.contact || 0
          };
          placements.push(pl);
          if (cb.onPlaced) {
            const pocket = coarse.largestPocket();
            cb.onPlaced(pl, {
              trial, placed: placements.length, total: pinnedFirst.length,
              freePt2: fine.freeCells() / (fineRes * fineRes),
              usablePt2: usableCellsFine / (fineRes * fineRes),
              placedPt2: placedCells / (fineRes * fineRes),
              pocket: { wPt: pocket.w / coarseRes, hPt: pocket.h / coarseRes, xPt: pocket.x / coarseRes, yPt: pocket.y / coarseRes }
            });
          }
        }
        await yieldNow();
      }

      const summary = {
        trial, placed: placements.length, total: pinnedFirst.length, rejects,
        density: placedCells / usableCellsFine,
        elapsedMs: now() - t0, gravW, noise
      };
      const better = !best || placements.length > best.placements.length ||
        (placements.length === best.placements.length && compactness(placements) < compactness(best.placements));
      if (better) {
        failStreak = 0;
        best = { placements, rejects, density: summary.density, trial, usablePt2: usableCellsFine / (fineRes * fineRes),
          freePt2: fine.freeCells() / (fineRes * fineRes), placedPt2: placedCells / (fineRes * fineRes),
          pocket: pocketPt(coarse, coarseRes) };
        if (cb.onBest) cb.onBest(best, summary);
      } else failStreak++;
      if (cb.onTrial) cb.onTrial(Object.assign({ better }, summary));
      if (placements.length === pinnedFirst.length) { endedBy = "complete"; break; }
      await yieldNow();
    }
    if (trials >= maxTrials && endedBy === "budget") endedBy = "trials";
    if (!best) best = { placements: [], rejects: prepared.map(p => p.id), density: 0, trial: -1, usablePt2: usableCellsFine / (fineRes * fineRes), freePt2: usableCellsFine / (fineRes * fineRes), placedPt2: 0, pocket: pocketPt(baseCoarse, coarseRes) };
    return Object.assign({}, best, {
      endedBy, trials, elapsedMs: now() - t0,
      params: { seed: job.seed == null ? 1 : job.seed, angles, clearancePt, insetPt, fineRes, coarseRes, pieceOrder: byAreaDesc.map(p => p.id) }
    });
  }

  /** Verification tolerance for a negative clearance, in pixels at `res`: what the
   *  solver eroded at its own resolution, plus one cell of the solver grid (the
   *  masks are conservative by up to one fine cell) and one pixel of raster slack. */
  function erosionPx(clearancePt, res, fineRes) {
    fineRes = fineRes || 2;
    const erodeFine = (+clearancePt || 0) < 0 ? Math.round(-clearancePt / 2 * fineRes) : 0;
    return erodeFine ? Math.ceil((erodeFine + 1) / fineRes * res) + 1 : 0;
  }
  function pocketPt(coarse, coarseRes) {
    const p = coarse.largestPocket();
    return { wPt: p.w / coarseRes, hPt: p.h / coarseRes, xPt: p.x / coarseRes, yPt: p.y / coarseRes };
  }

  /** Lower is tighter: the bounding box of everything placed, normalised. */
  function compactness(pl) {
    if (!pl.length) return Infinity;
    let x1 = 0, y1 = 0;
    for (const p of pl) { x1 = Math.max(x1, p.xPt + p.wPt); y1 = Math.max(y1, p.yPt + p.hPt); }
    return x1 * y1;
  }

  /** Majority-resample a fine mask to the coarse grid. */
  function majority(bits, w, h, k) {
    const W = Math.ceil(w / k), H = Math.ceil(h / k), cnt = new Uint16Array(W * H), tot = new Uint16Array(W * H);
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
      const i = ((y / k) | 0) * W + ((x / k) | 0); tot[i]++; if (bits[y * w + x]) cnt[i]++;
    }
    const out = new Uint8Array(W * H);
    for (let i = 0; i < out.length; i++) if (cnt[i] * 2 >= tot[i] && cnt[i]) out[i] = 1;
    return { bits: out, w: W, h: H };
  }

  /** Candidate search for one piece across all its angles. */
  function search(p, fine, coarse, ratio, gravW, noise, random, cornerX, cornerY) {
    const K = 28, TOL = 2;
    let best = null;
    const CW = coarse.W, CH = coarse.H;
    for (const v of p.variants) {
      // ── coarse exhaustive scan ──
      const cands = [];
      const cw = v.coarse.w, ch = v.coarse.h, maskCells = v.coarse.pm.cells, boxCells = cw * ch;
      const perim = 2 * (cw + ch) + 4;
      for (let y = 0; y + ch <= CH; y++) for (let x = 0; x + cw <= CW; x++) {
        // O(1) box tests first: the mask cannot fit if the box lacks free cells;
        // an empty box needs no bit test at all.
        const inner = coarse.boxSum(x, y, x + cw, y + ch);
        if (boxCells - inner < maskCells - TOL) continue;
        let ov = 0;
        if (inner > 0) { ov = coarse.overlap(v.coarse.pm, x, y, TOL); if (ov > TOL) continue; }
        const contact = (coarse.boxSum(x - 1, y - 1, x + cw + 1, y + ch + 1) - inner) / perim;
        const gx = cornerX ? (CW - x - cw) : x, gy = cornerY ? (CH - y - ch) : y;
        const s = contact - gravW * ((gx + gy) / (CW + CH)) - 0.05 * ov + (noise ? noise * random() : 0);
        if (cands.length < K) { cands.push({ x, y, s }); if (cands.length === K) cands.sort((a, b) => b.s - a.s); }
        else if (s > cands[K - 1].s) { cands[K - 1] = { x, y, s }; cands.sort((a, b) => b.s - a.s); }
      }
      if (!cands.length) continue;
      // ── fine refinement around each candidate ──
      const pm = v.fine.pm, FW = fine.W, FH = fine.H, rc = Math.max(1, v.ringFine.cells), pad = v.ringPad;
      const seen = new Set();
      for (const c of cands) {
        const x0 = c.x * ratio - ratio, y0 = c.y * ratio - ratio;
        for (let y = y0; y <= y0 + 2 * ratio; y++) for (let x = x0; x <= x0 + 2 * ratio; x++) {
          if (x < 0 || y < 0 || x + pm.w > FW || y + pm.h > FH) continue;
          const key = y * FW + x; if (seen.has(key)) continue; seen.add(key);
          if (!fine.fits(pm, x, y)) continue;
          const contact = fine.overlap(v.ringFine, x - pad, y - pad, 1e9) / rc;
          const gx = cornerX ? (FW - x - pm.w) : x, gy = cornerY ? (FH - y - pm.h) : y;
          const s = contact - gravW * ((gx + gy) / (FW + FH)) + (noise ? noise * random() : 0);
          if (!best || s > best.score) best = { v, x, y, score: s, contact };
        }
      }
    }
    if (best) return best;
    // ── fallback: widen the coarse tolerance so a tight fit is never missed ──
    for (const v of p.variants) {
      const pm = v.fine.pm, FW = fine.W, FH = fine.H, cw = v.coarse.w, ch = v.coarse.h, rc = Math.max(1, v.ringFine.cells), pad = v.ringPad;
      for (let cy = 0; cy + ch <= CH; cy++) for (let cx = 0; cx + cw <= CW; cx++) {
        if (cw * ch - coarse.boxSum(cx, cy, cx + cw, cy + ch) < v.coarse.pm.cells - 6) continue;
        if (coarse.overlap(v.coarse.pm, cx, cy, 6) > 6) continue;
        for (let y = cy * ratio - ratio; y <= cy * ratio + ratio; y++) for (let x = cx * ratio - ratio; x <= cx * ratio + ratio; x++) {
          if (x < 0 || y < 0 || x + pm.w > FW || y + pm.h > FH) continue;
          if (!fine.fits(pm, x, y)) continue;
          const contact = fine.overlap(v.ringFine, x - pad, y - pad, 1e9) / rc;
          const s = contact - gravW * ((x + y) / (FW + FH));
          if (!best || s > best.score) best = { v, x, y, score: s, contact };
        }
      }
    }
    return best;
  }

  /* ── independent verification ──────────────────────────────────────────
     Re-rasterises every placed silhouette (undilated) at `res` px/pt from
     the solver's own placements and checks pairwise overlap, sheet bounds
     and the minimum gaps. The browser additionally re-renders the written
     PDF (see charm-nest-pdf.js); this is the geometric half that both the
     browser and the Node fallback share.                                   */
  function verify(job, placements, res, erodeOverride) {
    res = res || 6;
    const erodePx = erodeOverride != null ? erodeOverride : erosionPx(job.clearancePt, res, job.fineRes || 2);
    const FW = Math.round(job.sheet.wPt * res), FH = Math.round(job.sheet.hPt * res);
    const ids = new Int16Array(FW * FH).fill(-1);
    const byId = new Map(job.pieces.map(p => [p.id, p]));
    let overlapPx = 0, outsidePx = 0; const pairs = new Set();
    const masks = [];
    placements.forEach((pl, i) => {
      const p = byId.get(pl.id); if (!p) return;
      const r = resample(p.bits, p.w, p.h, p.scale, res);
      const rot0 = rotateBitmap(r.bits, r.w, r.h, pl.angle);
      const rot = erodePx ? Object.assign(erode(rot0.bits, rot0.w, rot0.h, erodePx), { cx: rot0.cx - erodePx, cy: rot0.cy - erodePx }) : rot0;
      const x0 = Math.round(pl.cxPt * res - rot.cx), y0 = Math.round(pl.cyPt * res - rot.cy);
      masks.push({ i, id: pl.id, bits: rot.bits, w: rot.w, h: rot.h, x0, y0 });
      for (let y = 0; y < rot.h; y++) for (let x = 0; x < rot.w; x++) if (rot.bits[y * rot.w + x]) {
        const gx = x0 + x, gy = y0 + y;
        if (gx < 0 || gy < 0 || gx >= FW || gy >= FH) { outsidePx++; continue; }
        const k = gy * FW + gx;
        if (ids[k] >= 0 && ids[k] !== i) { overlapPx++; pairs.add(ids[k] < i ? ids[k] + ":" + i : i + ":" + ids[k]); }
        ids[k] = i;
      }
    });
    // min gap: chamfer distance to the nearest *other* piece, sampled at the boundary
    let minGapPt = Infinity, minEdgePt = Infinity;
    const near = nearestOther(ids, FW, FH, masks);
    for (const m of masks) {
      minGapPt = Math.min(minGapPt, near.get(m.i) / res);
      minEdgePt = Math.min(minEdgePt, m.x0 / res, m.y0 / res, (FW - m.x0 - m.w) / res, (FH - m.y0 - m.h) / res);
    }
    return {
      ok: overlapPx === 0 && outsidePx === 0,
      overlapPx, outsidePx, res,
      overlappingPairs: [...pairs].map(s => s.split(":").map(n => placements[+n].id)),
      minGapPt: masks.length > 1 ? +minGapPt.toFixed(3) : null,
      minEdgePt: masks.length ? +minEdgePt.toFixed(3) : null
    };
  }

  /** For each piece, the smallest distance from its cells to any other piece's cells. */
  function nearestOther(ids, W, H, masks) {
    const out = new Map();
    // boundary pixels per piece
    const bounds = new Map();
    for (const m of masks) {
      const pts = [];
      for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) if (m.bits[y * m.w + x]) {
        const e = x === 0 || y === 0 || x === m.w - 1 || y === m.h - 1 || !m.bits[y * m.w + x - 1] || !m.bits[y * m.w + x + 1] || !m.bits[(y - 1) * m.w + x] || !m.bits[(y + 1) * m.w + x];
        if (e) pts.push(m.x0 + x, m.y0 + y);
      }
      bounds.set(m.i, pts);
    }
    for (const m of masks) {
      const mine = bounds.get(m.i); let bestD2 = Infinity;
      for (const o of masks) {
        if (o.i === m.i) continue;
        // quick box reject beyond the current best
        const gap = boxGap(m, o); if (gap * gap > bestD2) continue;
        const theirs = bounds.get(o.i);
        for (let a = 0; a < mine.length; a += 2) for (let b = 0; b < theirs.length; b += 2) {
          const dx = mine[a] - theirs[b], dy = mine[a + 1] - theirs[b + 1], d2 = dx * dx + dy * dy;
          if (d2 < bestD2) bestD2 = d2;
        }
      }
      out.set(m.i, Math.sqrt(bestD2));
    }
    return out;
  }
  function boxGap(a, b) {
    const dx = Math.max(0, Math.max(a.x0, b.x0) - Math.min(a.x0 + a.w, b.x0 + b.w));
    const dy = Math.max(0, Math.max(a.y0, b.y0) - Math.min(a.y0 + a.h, b.y0 + b.h));
    return Math.sqrt(dx * dx + dy * dy);
  }

  function now() { return (typeof performance !== "undefined" && performance.now) ? performance.now() : Date.now(); }

  /* ── tools for the AI placer: exact collision grid + one mask per (piece, angle) ── */
  function makeSheetGrid(sheet, clearancePt, fineRes) {
    fineRes = fineRes || 2;
    const FW = Math.round(sheet.wPt * fineRes), FH = Math.round(sheet.hPt * fineRes);
    const insetPt = sheet.insetPt == null ? 1.5 : +sheet.insetPt;
    const halfGap = clearancePt >= 0 ? Math.ceil(clearancePt / 2 * fineRes) : 0;
    const wall = Math.round(insetPt * fineRes) + halfGap;
    const g = new Grid(FW, FH);
    for (let y = 0; y < FH; y++) for (let x = 0; x < FW; x++) { if (x < wall || y < wall || x >= FW - wall || y >= FH - wall) g.set(x, y); else g.free[y * FW + x] = 1; }
    g.buildSAT(); g.fineRes = fineRes; g.usableCells = g.freeCells();
    return g;
  }
  function prepareVariant(piece, angle, clearancePt, fineRes) {
    fineRes = fineRes || 2;
    const halfGap = clearancePt >= 0 ? Math.ceil(clearancePt / 2 * fineRes) : 0;
    const erodeFine = clearancePt < 0 ? Math.round(-clearancePt / 2 * fineRes) : 0;
    const fine = resample(piece.bits, piece.w, piece.h, piece.scale, fineRes);
    const rot = rotateBitmap(fine.bits, fine.w, fine.h, ((angle % 360) + 360) % 360);
    if (!rot.w) return null;
    const dil = erodeFine ? erode(rot.bits, rot.w, rot.h, erodeFine) : dilate(rot.bits, rot.w, rot.h, halfGap);
    if (!dil.w || !areaOf(dil.bits)) return null;
    return { angle, fine: { bits: dil.bits, w: dil.w, h: dil.h, pm: packShifted(dil.bits, dil.w, dil.h) }, solid: { w: rot.w, h: rot.h, cx: rot.cx + halfGap - erodeFine, cy: rot.cy + halfGap - erodeFine }, cells: areaOf(rot.bits) };
  }
  /** Try a requested centre (pt, y-down); if it collides, slide to the nearest legal spot within snapPt. */
  function tryPlace(grid, v, cxPt, cyPt, snapPt) {
    const res = grid.fineRes;
    const x0 = Math.round(cxPt * res - v.solid.cx), y0 = Math.round(cyPt * res - v.solid.cy);
    if (grid.fits(v.fine.pm, x0, y0)) return { ok: true, x: x0, y: y0, snapped: 0 };
    const R = Math.round((snapPt || 0) * res);
    let best = null;
    for (let r = 1; r <= R; r++) {                       // rings outward: nearest legal spot wins
      for (let dy = -r; dy <= r; dy++) for (let dx = -r; dx <= r; dx++) {
        if (Math.max(Math.abs(dx), Math.abs(dy)) !== r) continue;
        const d = dx * dx + dy * dy; if (best && d >= best.d) continue;
        if (grid.fits(v.fine.pm, x0 + dx, y0 + dy)) best = { x: x0 + dx, y: y0 + dy, d };
      }
      if (best) break;
    }
    if (best) return { ok: true, x: best.x, y: best.y, snapped: Math.sqrt(best.d) / res };
    // why it failed: off the sheet, or how many cells collide at the requested spot
    const off = x0 < 0 || y0 < 0 || x0 + v.fine.w > grid.W || y0 + v.fine.h > grid.H;
    const overlap = off ? null : grid.overlap(v.fine.pm, x0, y0, 1e9);
    return { ok: false, x: x0, y: y0, off, overlapPt2: overlap == null ? null : overlap / (res * res) };
  }
  function stampVariant(grid, v, x, y) { grid.stamp(v.fine.bits, v.fine.w, v.fine.h, x, y); grid.buildSAT(); }
  return { solve, verify, erosionPx, rotateBitmap, dilate, erode, ring, resample, packShifted, Grid, rng, popcount32, makeSheetGrid, prepareVariant, tryPlace, stampVariant };
});
