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
 *  Sparse queues instead minimise growth of an occupied strip from the left
 *  (top on portrait stock). Contact breaks ties inside that strip. A handful
 *  of restarts compact the result even after all pieces fit, preserving a
 *  rectangular offcut without changing any piece's dimensions or clearance.
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
    /* A search that has not beaten its best for a while is done: on a sheet given more pieces than it can hold, the
       ceiling used to be spent trying to seat pieces that could not fit, minutes after the layout had settled. */
    const stallMs = +job.stallMs || 0; let lastBetterAt = now(), stalled = false; let best = null;
    const stopped = () => (cb.shouldStop && cb.shouldStop()) || (now() - t0) > budget || (stalled = !!(stallMs && !job.packingPending && best && (now() - lastBetterAt) > stallMs));

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
          fine: { bits: dil.bits, w: dil.w, h: dil.h, pm: packShifted(dil.bits, dil.w, dil.h) }, cells: areaOf(dil.bits),
          solid: { bits: rot.bits, w: rot.w, h: rot.h, cx: rot.cx + halfGapFine - erodeFine, cy: rot.cy + halfGapFine - erodeFine },
          ringFine: packShifted(rg.bits, rg.w, rg.h), ringPad: Math.max(2, Math.round(2 * fineRes)),
          coarse: { pm: packShifted(co.bits, co.w, co.h), w: co.w, h: co.h }
        });
      }
      // footprintCells: what the piece occupies on the sheet grid (its eroded or grown mask) — the same measure the
      // occupancy readout uses, so the fill ceiling and "% full" agree. The solid silhouette (holes filled) stays for reports.
      const footprintCells = variants.length ? Math.min(...variants.map(v => v.cells)) : solidFineCells;
      prepared.push({ id: p.id, idx: i, order: p.order || p.id, orderDate: +p.orderDate || 0, areaPt2: p.areaPt2 || (solidFineCells / (fineRes * fineRes)), solidFineCells, footprintCells, variants, pinned: p.pinned || null, meta: p.meta || null });
      if (cb.onStage) cb.onStage("prepare", i + 1, pieces.length);
      await yieldNow();
    }

    const maxFill = job.maxFill > 0 && job.maxFill < 1 ? job.maxFill : 1;   // hard ceiling on fill (solid cells / usable cells)
    const stripAxis = FW >= FH ? "x" : "y";
    const stripOf = rec => ({ axis: stripAxis, end: rec.reduce((n, r) => Math.max(n, stripAxis === "x" ? r.x + r.v.fine.w : r.y + r.v.fine.h), wallFine) });

    /* ── finishing push ─────────────────────────────────────────────────────
       A layout one or two pieces short gets a targeted search for exactly those
       pieces, at 5° steps, into that layout — instead of hoping a further blind
       restart lands the gap. Runs on every new best that qualifies; if it fails,
       the restarts simply continue.                                             */
    async function finishPush(best) {
      const missing = repairCandidates(best);
      if (!missing.length || missing.length > 2 || !best.grids || best.pushed) return;
      if (cb.shouldStop && cb.shouldStop()) return;
      best.pushed = true;
      const fine = best.grids.fine, coarse = best.grids.coarse;
      const total = prepared.length;
      for (const id of missing) {
        if (stopped()) break;
        const p = prepared.find(x => x.id === id); if (!p || p.pinned || multi(p)) continue;
        if ((best.placedCells + p.footprintCells) / usableCellsFine > maxFill) continue;
        if (cb.onStage) cb.onStage("finish", best.placements.length, total);
        const variants = p.variants.slice();
        const fineBase = resample(pieces[p.idx].bits, pieces[p.idx].w, pieces[p.idx].h, pieces[p.idx].scale, fineRes);
        for (let a = 0; a < 360; a += 5) {
          if (angles.includes(a)) continue;
          const rot = rotateBitmap(fineBase.bits, fineBase.w, fineBase.h, a); if (!rot.w) continue;
          const dil = erodeFine ? erode(rot.bits, rot.w, rot.h, erodeFine) : dilate(rot.bits, rot.w, rot.h, halfGapFine);
          if (!dil.w || !areaOf(dil.bits)) continue;
          const rg = ring(dil.bits, dil.w, dil.h, Math.max(2, Math.round(2 * fineRes)));
          const co = majority(dil.bits, dil.w, dil.h, ratio);
          variants.push({ angle: a, fine: { bits: dil.bits, w: dil.w, h: dil.h, pm: packShifted(dil.bits, dil.w, dil.h) }, cells: areaOf(dil.bits), solid: { bits: rot.bits, w: rot.w, h: rot.h, cx: rot.cx + halfGapFine - erodeFine, cy: rot.cy + halfGapFine - erodeFine }, ringFine: packShifted(rg.bits, rg.w, rg.h), ringPad: Math.max(2, Math.round(2 * fineRes)), coarse: { pm: packShifted(co.bits, co.w, co.h), w: co.w, h: co.h } });
        }
        const pos = search({ variants }, fine, coarse, ratio, 0.35, 0, random, 0, 0, best.stripPacked ? stripOf(best.rec) : null);
        if (!pos) continue;
        const { v, x, y } = pos;
        if ((best.placedCells + v.cells) / usableCellsFine > maxFill) continue;
        fine.stamp(v.fine.bits, v.fine.w, v.fine.h, x, y);
        for (let r = 0; r < v.fine.h; r++) for (let c = 0; c < v.fine.w; c++) if (v.fine.bits[r * v.fine.w + c]) { const gx = Math.floor((x + c) / ratio), gy = Math.floor((y + r) / ratio); if (gx >= 0 && gy >= 0 && gx < coarse.W && gy < coarse.H) coarse.set(gx, gy); }
        coarse.buildSAT();
        const pl = { id, angle: v.angle, cxPt: (x + v.solid.cx) / fineRes, cyPt: (y + v.solid.cy) / fineRes, xPt: (x + (v.fine.w - v.solid.w) / 2) / fineRes, yPt: (y + (v.fine.h - v.solid.h) / 2) / fineRes, wPt: v.solid.w / fineRes, hPt: v.solid.h / fineRes, contact: pos.contact || 0, finishing: true };
        best.rec.push({ p, v, x, y });
        best.placements.push(pl); best.rejects = best.rejects.filter(r => r !== id);
        best.placedCells += v.cells; best.placedPt2 = best.placedCells / (fineRes * fineRes);
        best.density = best.placedCells / usableCellsFine; best.freePt2 = fine.freeCells() / (fineRes * fineRes); best.pocket = pocketPt(coarse, coarseRes);
        if (cb.onPlaced) cb.onPlaced(pl, { trial: best.trial, placed: best.placements.length, total, freePt2: best.freePt2, usablePt2: usableCellsFine / (fineRes * fineRes), placedPt2: best.placedPt2, pocket: best.pocket, finishing: true });
        if (cb.onBest) cb.onBest(best, { trial: best.trial, placed: best.placements.length, total, rejects: best.rejects, density: best.density, elapsedMs: now() - t0, finishing: true });
        await yieldNow();
      }
    }

    /* ── ruin & recreate ─────────────────────────────────────────────────────
       Repair the best layout instead of discarding it: pull out the pieces around
       its largest gap (or a random neighbourhood), then re-insert them together
       with the missing pieces, missing pieces first, at 15° steps. Deterministic
       given the seed. Invoked when restarts stall and the best is a few short.  */
    const variantsFor = (p, list) => {
        if (!p._v15) { p._v15 = new Map(); for (const v of p.variants) p._v15.set(v.angle, v); }
        const out = [];
        for (const a of list) {
          if (!p._v15.has(a)) {
            const fb = resample(pieces[p.idx].bits, pieces[p.idx].w, pieces[p.idx].h, pieces[p.idx].scale, fineRes);
            const rot = rotateBitmap(fb.bits, fb.w, fb.h, a); if (!rot.w) { p._v15.set(a, null); continue; }
            const dil = erodeFine ? erode(rot.bits, rot.w, rot.h, erodeFine) : dilate(rot.bits, rot.w, rot.h, halfGapFine);
            if (!dil.w || !areaOf(dil.bits)) { p._v15.set(a, null); continue; }
            const rg = ring(dil.bits, dil.w, dil.h, Math.max(2, Math.round(2 * fineRes)));
            const co = majority(dil.bits, dil.w, dil.h, ratio);
            p._v15.set(a, { angle: a, fine: { bits: dil.bits, w: dil.w, h: dil.h, pm: packShifted(dil.bits, dil.w, dil.h) }, cells: areaOf(dil.bits), solid: { bits: rot.bits, w: rot.w, h: rot.h, cx: rot.cx + halfGapFine - erodeFine, cy: rot.cy + halfGapFine - erodeFine }, ringFine: packShifted(rg.bits, rg.w, rg.h), ringPad: Math.max(2, Math.round(2 * fineRes)), coarse: { pm: packShifted(co.bits, co.w, co.h), w: co.w, h: co.h } });
          }
          const v = p._v15.get(a); if (v) out.push(v);
        }
        return out;
      };

    async function ruinRecreate(best, attempts) {
      const missingIds = repairCandidates(best);
      if (!best?.rec?.length || !missingIds.length || missingIds.length > 3) return false;
      const total = prepared.length;
      const angles15 = []; for (let a = 0; a < 360; a += 15) angles15.push(a);
      for (let att = 0; att < attempts; att++) {
        if (stopped()) return false;
        // choose the neighbourhood: around the largest gap on even attempts, around a random piece on odd ones
        const rec = best.rec;
        let anchor;
        if (att % 2 === 0) { const pk = best.grids.coarse.largestPocket(); anchor = { x: (pk.x + pk.w / 2) * ratio, y: (pk.y + pk.h / 2) * ratio }; }
        else { const r = rec[Math.floor(random() * rec.length)]; anchor = { x: r.x + r.v.fine.w / 2, y: r.y + r.v.fine.h / 2 }; }
        const k = 2 + Math.floor(random() * 3);                       // remove 2–4 pieces
        const byDist = rec.slice().sort((a, b) => Math.hypot(a.x + a.v.fine.w / 2 - anchor.x, a.y + a.v.fine.h / 2 - anchor.y) - Math.hypot(b.x + b.v.fine.w / 2 - anchor.x, b.y + b.v.fine.h / 2 - anchor.y));
        const removed = byDist.slice(0, k).filter(r => !r.p.pinned);
        const keep = rec.filter(r => !removed.includes(r));
        // rebuild grids from the kept pieces
        const fine = baseFine.clone(), coarse = baseCoarse.clone();
        for (const r of keep) { fine.stamp(r.v.fine.bits, r.v.fine.w, r.v.fine.h, r.x, r.y); for (let yy = 0; yy < r.v.fine.h; yy++) for (let xx = 0; xx < r.v.fine.w; xx++) if (r.v.fine.bits[yy * r.v.fine.w + xx]) { const gx = Math.floor((r.x + xx) / ratio), gy = Math.floor((r.y + yy) / ratio); if (gx >= 0 && gy >= 0 && gx < coarse.W && gy < coarse.H) coarse.set(gx, gy); } }
        coarse.buildSAT();
        // re-insert: missing pieces first, then the removed ones, largest first, at 15° steps
        const missing = missingIds.map(id => prepared.find(x => x.id === id)).filter(Boolean);
        if (missing.some(p => p.pinned)) continue;
        const queue = missing.concat(removed.map(r => r.p).sort((a, b) => b.areaPt2 - a.areaPt2));
        const newRec = keep.slice(); let ok = true, cells = keep.reduce((n,r) => n + r.v.cells, 0);
        for (const p of queue) {
          if (stopped()) { ok = false; break; }
          const pos = search({ variants: variantsFor(p, angles15) }, fine, coarse, ratio, 0.35, 0.02, random, 0, 0, best.stripPacked ? stripOf(newRec) : null);
          if (!pos) { ok = false; break; }
          const { v, x, y } = pos;
          if ((cells + v.cells) / usableCellsFine > maxFill) { ok = false; break; }
          cells += v.cells;
          fine.stamp(v.fine.bits, v.fine.w, v.fine.h, x, y);
          for (let yy = 0; yy < v.fine.h; yy++) for (let xx = 0; xx < v.fine.w; xx++) if (v.fine.bits[yy * v.fine.w + xx]) { const gx = Math.floor((x + xx) / ratio), gy = Math.floor((y + yy) / ratio); if (gx >= 0 && gy >= 0 && gx < coarse.W && gy < coarse.H) coarse.set(gx, gy); }
          coarse.buildSAT();
          newRec.push({ p, v, x, y });
        }
        if (cb.onStage) cb.onStage("repair", att + 1, attempts);
        await yieldNow();
        if (!ok) continue;
        // success: everything placed — rebuild the public layout from newRec
        const placements = newRec.map(r => ({ id: r.p.id, angle: r.v.angle, cxPt: (r.x + r.v.solid.cx) / fineRes, cyPt: (r.y + r.v.solid.cy) / fineRes, xPt: (r.x + (r.v.fine.w - r.v.solid.w) / 2) / fineRes, yPt: (r.y + (r.v.fine.h - r.v.solid.h) / 2) / fineRes, wPt: r.v.solid.w / fineRes, hPt: r.v.solid.h / fineRes, repaired: true }));
        const placedCells = newRec.reduce((n, r) => n + r.v.cells, 0);
        const placedIds = new Set(placements.map(p => p.id));
        const rejects = prepared.filter(p => !placedIds.has(p.id)).map(p => p.id);
        Object.assign(best, { placements, rejects, capped: (best.capped || []).filter(id => !placedIds.has(id)), density: placedCells / usableCellsFine, placedCells, placedPt2: placedCells / (fineRes * fineRes), freePt2: fine.freeCells() / (fineRes * fineRes), pocket: pocketPt(coarse, coarseRes), grids: { fine, coarse }, rec: newRec, repaired: true });
        if (cb.onBest) cb.onBest(best, { trial: best.trial, placed: placements.length, total, rejects, density: best.density, elapsedMs: now() - t0, repaired: true, removed: removed.length });
        return true;
      }
      return false;
    }

    /* ── trials ─────────────────────────────────────────────────────────── */
    best = null; let trials = 0, endedBy = "budget", failStreak = 0, lastRejects = [], lastAdvice = null, guidedTrials = 0;
    const fifo = prepared.some(p => p.orderDate > 0);
    const dates = new Map(); for (const p of prepared) dates.set(p.order, Math.min(dates.get(p.order) ?? Infinity, p.orderDate));
    const rank = new Map([...dates].sort((a, b) => a[1] - b[1] || String(a[0]).localeCompare(String(b[0]))).map(([id], i) => [id, i]));
    // Dates choose sheet membership, never the geometric placement sequence.
    // Admit a whole chronological prefix under the area cap, then let shapes interlock.
    let capOrders = rank.size, admittedCells = 0;
    if (fifo) for (const [id, r] of rank) {
      const cells = prepared.filter(p => p.order === id).reduce((n,p) => n + p.footprintCells, 0);
      if ((admittedCells + cells) / usableCellsFine > maxFill) { capOrders = r; break; }
      admittedCells += cells;
    }
    const prefixCount = layout => layout?.placements.length ? new Set(layout.placements.map(pl => prepared.find(p => p.id === pl.id).order)).size : 0;
    function repairCandidates(layout) {
      if (!layout?.rejects?.length) return [];
      if (!fifo) return layout.rejects.filter(id => !(layout.capped || []).includes(id));
      const next = Math.min(...layout.rejects.map(id => rank.get(prepared.find(p => p.id === id).order)));
      if (next >= capOrders) return [];
      // No repair may let a younger order jump over the first waiting order.
      return prepared.filter(p => rank.get(p.order) === next).map(p => p.id);
    }
    const byAreaDesc = prepared.slice().sort((a, b) => b.areaPt2 - a.areaPt2);
    // orders: pieces sharing `order` travel together — a sheet never holds part of a multi-piece order
    const orderSize = new Map(); for (const p of prepared) orderSize.set(p.order, (orderSize.get(p.order) || 0) + 1);
    const multi = (p) => (orderSize.get(p.order) || 1) > 1;
    const rebuildGrids = (rec) => {
      const fine = baseFine.clone(), coarse = baseCoarse.clone();
      for (const r of rec) { fine.stamp(r.v.fine.bits, r.v.fine.w, r.v.fine.h, r.x, r.y); for (let yy = 0; yy < r.v.fine.h; yy++) for (let xx = 0; xx < r.v.fine.w; xx++) if (r.v.fine.bits[yy * r.v.fine.w + xx]) { const gx = Math.floor((r.x + xx) / ratio), gy = Math.floor((r.y + yy) / ratio); if (gx >= 0 && gy >= 0 && gx < coarse.W && gy < coarse.H) coarse.set(gx, gy); } }
      coarse.buildSAT(); return { fine, coarse };
    };
    while (trials < maxTrials) {
      if (job.packingHints && job.packingHints !== lastAdvice) { lastAdvice = job.packingHints; lastBetterAt = now(); }
      if (stopped()) { endedBy = cb.shouldStop && cb.shouldStop() ? "stopped" : stalled ? "stalled" : "budget"; break; }
      const trial = trials++;
      // ordering: trial 0 = pure largest-first; later trials add noise growing with the streak
      const sigma = trial === 0 ? 0 : Math.min(0.6, 0.15 + 0.05 * Math.min(failStreak, 8));
      // pieces the previous trial could not place go first (most of the time), so the layout is built around them
      const front = trial > 0 && lastRejects.length && random() < 0.75 ? new Set(lastRejects) : new Set();
      const targetOrders = fifo && best && failStreak >= 2 && trial % 4 !== 0 ? Math.min(capOrders, prefixCount(best) + 1) : capOrders;
      const candidates = fifo ? byAreaDesc.filter(p => rank.get(p.order) < targetOrders) : byAreaDesc;
      // Sparse queues should consume a strip from one edge, leaving a rectangular
      // offcut. Full-sheet searches retain contact scoring; alternate restarts
      // can still recover an awkward mix that did not fit the compact first pass.
      const sparse = candidates.reduce((n, p) => n + p.footprintCells, 0) / usableCellsFine < maxFill * 0.9;
      const stripPacked = (sparse || (trial > 0 && best?.density < maxFill * 0.9)) && trial % 3 !== 2;
      const advice = job.packingHints || {}, priorities = new Map((advice.priority || []).map((id,i,a) => [id, 1 - i / Math.max(1,a.length)]));
      const guided = trial % 3 === 1 && priorities.size;
      if (guided) guidedTrials++;
      if (guided) for (const p of candidates) if (!p.pinned && advice.angles?.[p.id]) p.variants = variantsFor(p, [...new Set(angles.concat(advice.angles[p.id].filter(Number.isFinite).slice(0,3).map(a => ((a % 360) + 360) % 360)))]);
      const order = candidates.map(p => ({ p, k: (front.has(p.id) ? 1e9 : 0) + p.areaPt2 * (1 + sigma * (random() * 2 - 1) + (guided ? .8 * (priorities.get(p.id) || 0) : 0)) })).sort((a, b) => b.k - a.k).map(o => o.p);
      const pinnedFirst = order.filter(p => p.pinned).concat(order.filter(p => !p.pinned));
      const gravW = trial === 0 ? 0.35 : 0.1 + random() * 0.8;
      const noise = trial === 0 ? 0 : random() * 0.15;
      const cornerX = trial === 0 ? 0 : (random() < 0.7 ? 0 : 1), cornerY = trial === 0 ? 0 : (random() < 0.7 ? 0 : 1);
      let fine = baseFine.clone(), coarse = baseCoarse.clone();
      let placements = [], placedRec = []; const capped = fifo ? prepared.filter(p => rank.get(p.order) >= capOrders).map(p => p.id) : [], rejects = capped.slice();
      let placedCells = 0;
      const deadOrders = new Set();
      // a piece of a multi-piece order failed: the order leaves this sheet whole — its placed siblings are lifted back off
      const dropOrder = (p, why) => {
        if (!multi(p)) return;
        deadOrders.add(p.order);
        const lifted = placedRec.filter(r => r.p.order === p.order);
        if (!lifted.length) return;
        placedRec = placedRec.filter(r => r.p.order !== p.order);
        placements = placements.filter(pl => !lifted.some(r => r.p.id === pl.id));
        for (const r of lifted) { placedCells -= r.v.cells; rejects.push(r.p.id); if (why === "cap") capped.push(r.p.id); if (cb.onReject) cb.onReject(r.p.id, trial, "order"); }
        ({ fine, coarse } = rebuildGrids(placedRec));
      };

      for (let pi = 0; pi < pinnedFirst.length; pi++) {
        const p = pinnedFirst[pi];
        if (stopped()) break;
        let bestPos = null;
        if (deadOrders.has(p.order)) { rejects.push(p.id); if (capped.some(id => prepared.find(x => x.id === id && x.order === p.order))) capped.push(p.id); if (cb.onReject) cb.onReject(p.id, trial, "order"); continue; }
        if (!p.pinned && (placedCells + p.footprintCells) / usableCellsFine > maxFill) {
          rejects.push(p.id); capped.push(p.id);
          if (cb.onReject) cb.onReject(p.id, trial, "cap");
          dropOrder(p, "cap");
          await yieldNow(); continue;
        }
        if (p.pinned) {
          const v = p.variants[0];
          const x = Math.round(p.pinned.cxPt * fineRes - v.solid.cx), y = Math.round(p.pinned.cyPt * fineRes - v.solid.cy);
          if (fine.fits(v.fine.pm, x, y)) bestPos = { v, x, y, score: Infinity };
        } else {
          bestPos = search(p, fine, coarse, ratio, gravW, noise, random, stripPacked ? 0 : cornerX, stripPacked ? 0 : cornerY, stripPacked ? stripOf(placedRec) : null);
        }
        if (bestPos && !p.pinned && (placedCells + bestPos.v.cells) / usableCellsFine > maxFill) {
          rejects.push(p.id); capped.push(p.id); dropOrder(p, "cap"); await yieldNow(); continue;
        }
        if (!bestPos) {
          rejects.push(p.id);
          if (cb.onReject) cb.onReject(p.id, trial, "nofit");
          dropOrder(p, "nofit");
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
          placedCells += v.cells;
          placedRec.push({ p, v, x, y });
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
              trial, placed: placements.length, total: prepared.length,
              freePt2: fine.freeCells() / (fineRes * fineRes),
              usablePt2: usableCellsFine / (fineRes * fineRes),
              placedPt2: placedCells / (fineRes * fineRes),
              pocket: { wPt: pocket.w / coarseRes, hPt: pocket.h / coarseRes, xPt: pocket.x / coarseRes, yPt: pocket.y / coarseRes }
            });
          }
        }
        await yieldNow();
      }

      // A budget/stop during a trial must not make the unvisited pieces disappear.
      for (const p of prepared) if (!placements.some(pl => pl.id === p.id) && !rejects.includes(p.id)) rejects.push(p.id);
      // Pinned pieces are placed first geometrically, but younger orders must not bypass an older rejection.
      if (fifo && rejects.length) {
        const cutoff = Math.min(...rejects.map(id => rank.get(prepared.find(p => p.id === id).order)));
        const lifted = placedRec.filter(r => rank.get(r.p.order) >= cutoff);
        if (lifted.length) {
          placedRec = placedRec.filter(r => !lifted.includes(r));
          placements = placements.filter(pl => !lifted.some(r => r.p.id === pl.id));
          for (const r of lifted) { placedCells -= r.v.cells; if (!rejects.includes(r.p.id)) rejects.push(r.p.id); }
          ({ fine, coarse } = rebuildGrids(placedRec));
        }
      }
      lastRejects = rejects.filter(id => !capped.includes(id));
      const summary = {
        trial, placed: placements.length, total: prepared.length, rejects,
        density: placedCells / usableCellsFine,
        elapsedMs: now() - t0, gravW, noise
      };
      const better = betterLayout({ placements, density: summary.density }, best, job.sheet);
      if (better) {
        failStreak = 0; lastBetterAt = now();
        best = { placements, rejects, capped: capped.slice(), density: summary.density, trial, stripPacked, usablePt2: usableCellsFine / (fineRes * fineRes),
          freePt2: fine.freeCells() / (fineRes * fineRes), placedPt2: placedCells / (fineRes * fineRes), placedCells,
          pocket: pocketPt(coarse, coarseRes), grids: { fine, coarse }, rec: placedRec.slice() };
        if (cb.onBest) cb.onBest(best, summary);
        // one or two short (and not because of the cap): targeted push into this very layout
        if (repairCandidates(best).length <= 2) await finishPush(best);
      } else failStreak++;
      if (cb.onTrial) cb.onTrial(Object.assign({ better }, summary));
      if (best && best.placements.length === prepared.length) {
        endedBy = "complete";
        // Seating every piece is not enough on a partial sheet: spend a few
        // bounded restarts shortening the occupied strip before publishing it.
        if (!sparse || trials >= Math.min(8, maxTrials)) break;
      }
      // restarts stalling while the best is a few short → repair the best layout instead
      if (best && repairCandidates(best).length > 0 && repairCandidates(best).length <= 3 && failStreak >= 4) {
        failStreak = 0;
        if (cb.onStage) cb.onStage("repair", 0, 6);
        const fixed = await ruinRecreate(best, 6);
        if (fixed && best.placements.length === prepared.length) { endedBy = "complete"; break; }
      }
      if (best && best.rejects.length && best.rejects.every(id => best.capped.includes(id))) { endedBy = "cap"; if (!sparse || trials >= Math.min(8, maxTrials)) break; }   // only the ceiling holds pieces back
      await yieldNow();
    }
    if (trials >= maxTrials && endedBy === "budget") endedBy = "trials";
    if (best && best.grids) delete best.grids;
    if (best && best.rec) delete best.rec;
    if (!best) best = { placements: [], rejects: prepared.map(p => p.id), density: 0, trial: -1, usablePt2: usableCellsFine / (fineRes * fineRes), freePt2: usableCellsFine / (fineRes * fineRes), placedPt2: 0, pocket: pocketPt(baseCoarse, coarseRes) };
    const placedIds = new Set(best.placements.map(p => p.id));
    best.rejects = pieces.filter(p => !placedIds.has(p.id)).map(p => p.id);
    // safety: no sheet holds part of a multi-piece order — any order with a rejected piece leaves whole
    {
      const rej = new Set(best.rejects || []); const badOrders = new Set();
      for (const id of rej) { const p = prepared.find(x => x.id === id); if (p && multi(p)) badOrders.add(p.order); }
      if (badOrders.size) {
        const lifted = best.placements.filter(pl => { const p = prepared.find(x => x.id === pl.id); return p && badOrders.has(p.order); });
        if (lifted.length) {
          best.placements = best.placements.filter(pl => !lifted.includes(pl));
          best.rejects = (best.rejects || []).concat(lifted.map(pl => pl.id));
          const cells = lifted.reduce((n, pl) => { const p = prepared.find(x => x.id === pl.id); return n + (p ? p.footprintCells : 0); }, 0);
          best.placedCells = Math.max(0, (best.placedCells || 0) - cells); best.placedPt2 = best.placedCells / (fineRes * fineRes); best.density = best.placedCells / usableCellsFine;
          best.liftedForOrders = lifted.map(pl => pl.id);
        }
      }
    }
    // what the pieces the ceiling held back would add, so the console can say the arithmetic plainly
    const cappedPt2 = (best.capped || []).reduce((n, id) => { const p = prepared.find(x => x.id === id); return n + (p ? p.footprintCells / (fineRes * fineRes) : 0); }, 0);
    return Object.assign({}, best, {
      endedBy, trials, elapsedMs: now() - t0, cappedPt2,
      params: { compactPartial: !!best.stripPacked, packingAxis: best.stripPacked ? stripAxis : null, packingGuided: guidedTrials > 0, guidedTrials, seed: job.seed == null ? 1 : job.seed, angles, clearancePt, insetPt, fineRes, coarseRes, maxFill, pieceOrder: byAreaDesc.map(p => p.id) }
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

  /** Fraction consumed before a straight cut frees a full-width/height offcut.
   * Compare identical counts by usable leftover stock, not tiny raster-area gains. */
  function stripFraction(pl, sheet) {
    if (!pl.length || !sheet?.wPt || !sheet?.hPt) return Infinity;
    let x0 = Infinity, y0 = Infinity, x1 = 0, y1 = 0;
    for (const p of pl) {
      if (![p.xPt,p.yPt,p.wPt,p.hPt].every(Number.isFinite)) return Infinity;
      x0 = Math.min(x0, p.xPt); y0 = Math.min(y0, p.yPt);
      x1 = Math.max(x1, p.xPt + p.wPt); y1 = Math.max(y1, p.yPt + p.hPt);
    }
    return Math.min(x1 / sheet.wPt, (sheet.wPt - x0) / sheet.wPt, y1 / sheet.hPt, (sheet.hPt - y0) / sheet.hPt);
  }
  function betterLayout(candidate, incumbent, sheet) {
    if (!incumbent) return true;
    if (candidate.placements.length !== incumbent.placements.length) return candidate.placements.length > incumbent.placements.length;
    const a = stripFraction(candidate.placements, sheet), b = stripFraction(incumbent.placements, sheet);
    if (a !== b) return a < b;
    return candidate.density > incumbent.density;
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
  function search(p, fine, coarse, ratio, gravW, noise, random, cornerX, cornerY, strip = null) {
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
        const growth = strip ? Math.max(strip.end / ratio, strip.axis === "x" ? x + cw : y + ch) : 0;
        const s = contact - 4 * growth - gravW * ((gx + gy) / (CW + CH)) - 0.05 * ov + (noise ? noise * random() : 0);
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
          const growth = strip ? Math.max(strip.end, strip.axis === "x" ? x + pm.w : y + pm.h) / ratio : 0;
          const s = contact - 4 * growth - gravW * ((gx + gy) / (FW + FH)) + (noise ? noise * random() : 0);
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
          const growth = strip ? Math.max(strip.end, strip.axis === "x" ? x + pm.w : y + pm.h) / ratio : 0;
          const s = contact - 4 * growth - gravW * ((x + y) / (FW + FH));
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
  /** Coarse copy of a fine grid (any occupied fine cell → occupied coarse cell). */
  function coarseFromFine(fine, ratio) {
    const CW = Math.ceil(fine.W / ratio), CH = Math.ceil(fine.H / ratio), g = new Grid(CW, CH);
    for (let y = 0; y < fine.H; y++) for (let x = 0; x < fine.W; x++) { if (fine.get(x, y)) g.set(Math.floor(x / ratio), Math.floor(y / ratio)); }
    for (let y = 0; y < CH; y++) for (let x = 0; x < CW; x++) if (!g.get(x, y)) g.free[y * CW + x] = 1;
    g.buildSAT(); return g;
  }
  /** Measurement for the AI placer: the tightest legal spot for a piece at each angle, best first.
   *  One exhaustive evaluation of the current sheet per angle — a ruler, not a trial. */
  function bestSpots(fine, variantsByAngle, K, opts) {
    opts = opts || {}; const ratio = opts.ratio || 4, res = fine.fineRes;
    const coarse = coarseFromFine(fine, ratio);
    const out = [];
    for (const v of variantsByAngle) {
      if (!v) continue;
      const ringR = Math.max(2, Math.round(2 * res));
      const rg = ring(v.fine.bits, v.fine.w, v.fine.h, ringR);
      const co = majority(v.fine.bits, v.fine.w, v.fine.h, ratio);
      const pv = { angle: v.angle, fine: v.fine, solid: v.solid, ringFine: packShifted(rg.bits, rg.w, rg.h), ringPad: ringR, coarse: { pm: packShifted(co.bits, co.w, co.h), w: co.w, h: co.h } };
      const pos = search({ variants: [pv] }, fine, coarse, ratio, 0.35, 0, () => 0.5, 0, 0);
      if (pos) out.push({ angle: v.angle, x: pos.x, y: pos.y, cxPt: (pos.x + v.solid.cx) / res, cyPt: (pos.y + v.solid.cy) / res, contact: pos.contact || 0 });
    }
    out.sort((a, b) => b.contact - a.contact);
    return out.slice(0, K || 3);
  }
  /** Within snapPt of the requested centre, the legal spot with the most contact (tight, and close to what was asked). */
  function tryPlaceTight(grid, v, cxPt, cyPt, snapPt) {
    const res = grid.fineRes;
    const x0 = Math.round(cxPt * res - v.solid.cx), y0 = Math.round(cyPt * res - v.solid.cy);
    const R = Math.round((snapPt || 0) * res);
    const ringR = Math.max(2, Math.round(2 * res));
    if (!v._ring) { const rg = ring(v.fine.bits, v.fine.w, v.fine.h, ringR); v._ring = packShifted(rg.bits, rg.w, rg.h); }
    const rc = Math.max(1, v._ring.cells);
    let best = null;
    for (let dy = -R; dy <= R; dy++) for (let dx = -R; dx <= R; dx++) {
      const d2 = dx * dx + dy * dy; if (d2 > R * R) continue;
      const x = x0 + dx, y = y0 + dy;
      if (!grid.fits(v.fine.pm, x, y)) continue;
      const contact = grid.overlap(v._ring, x - ringR, y - ringR, 1e9) / rc;
      const score = contact - 0.15 * Math.sqrt(d2) / Math.max(1, R);
      if (!best || score > best.score) best = { x, y, score, contact, d: Math.sqrt(d2) };
    }
    if (best) return { ok: true, x: best.x, y: best.y, snapped: best.d / res, contact: best.contact };
    const off = x0 < 0 || y0 < 0 || x0 + v.fine.w > grid.W || y0 + v.fine.h > grid.H;
    const overlap = off ? null : grid.overlap(v.fine.pm, x0, y0, 1e9);
    return { ok: false, x: x0, y: y0, off, overlapPt2: overlap == null ? null : overlap / (res * res) };
  }
  return { solve, verify, betterLayout, stripFraction, erosionPx, rotateBitmap, dilate, erode, ring, resample, packShifted, Grid, rng, popcount32, makeSheetGrid, prepareVariant, tryPlace, tryPlaceTight, stampVariant, bestSpots, coarseFromFine };
});
