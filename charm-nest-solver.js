/*  charm-nest-solver.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  Raster nesting solver for the Charm Nesting Station.
 *
 *  One file, three hosts: the browser Web Worker (importScripts), Node
 *  (require, for the charmNestSolve-background fallback and the unit test)
 *  and, if ever needed, a plain <script>. No dependencies.
 *
 *  THE SEARCH
 *  Pieces are solid cut silhouettes (bitmaps), largest area first. Neighbour
 *  searches reward distinct nearby charms and close contour contact. A
 *  boundary-aligned construction seed remains among the proposals to avoid
 *  losing capacity to a greedy local optimum; it does not determine the
 *  final quality score. Restarts reshuffle order and angles. Layout selection
 *  preserves piece count and usable offcuts, then rewards measured silhouette
 *  contact with a smaller edge-alignment bonus; final refinement improves both.
 *  Sparse queues instead minimise growth of an occupied strip from the left
 *  (top on portrait stock). Contact breaks ties inside that strip. On a
 *  partial Rose Gold sheet the next green line removes, line by line, all
 *  stock up to the farthest charm. There every other restart also charges
 *  a smaller cost for the area that line would add, so charms settle into
 *  the bays of the previous line instead of leaving gaps along it. Layouts
 *  still rank by charm count and a straight usable front first; the line
 *  area only decides between equal fronts, ahead of contact. A handful
 *  of restarts plus rotation refinement compact the result after all pieces fit, preserving a
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
  if (typeof module === "object" && module.exports) module.exports = factory(require("./charm-nest-rose.js"));
  else root.CharmNestSolver = factory(root.CharmNestRose);
})(typeof self !== "undefined" ? self : this, function (Rose) {
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
  Grid.prototype.set = function (x, y) { this.occ[y * this.words + (x >> 5)] |= (1 << (x & 31)) >>> 0; this.free[y * this.W + x] = 0; if (this.material) this.material.set(x, y); };
  Grid.prototype.get = function (x, y) { return (this.occ[y * this.words + (x >> 5)] >>> (x & 31)) & 1; };
  Grid.prototype.clone = function () { const g = new Grid(this.W, this.H); g.occ.set(this.occ); g.free.set(this.free); if (this.sat) g.sat = this.sat.slice(); if (this.material) g.material = this.material.clone(); if (this.parts) g.parts = this.parts.slice(); g.walls = this.walls; g.materialOnly = this.materialOnly; g.remnant = this.remnant; return g; };
  Grid.prototype.trackMaterial = function (owners = false) { this.walls = new Grid(this.W, this.H); this.walls.occ.set(this.occ); this.walls.materialOnly = true; this.material = new Grid(this.W, this.H); this.material.materialOnly = true; if (owners) this.parts = []; return this; };
  /** Legal position test: no overlap between the packed mask and the grid. */
  Grid.prototype.fits = function (pm, x, y) {
    if (x < 0 || y < 0 || x + pm.w > this.W || y + pm.h > this.H) return false;
    const v = pm.variants[x & 31], wx = x >> 5, gw = this.words, mw = pm.words, occ = this.occ;
    const wmax = Math.min(mw, gw - wx);
    for (let r = 0; r < pm.h; r++) {
      const gi = (y + r) * gw + wx, mi = r * mw;
      for (let k = 0; k < wmax; k++) if (occ[gi + k] & v[mi + k]) return false;
    }
    return !this.remnant || !pm.outline || clearOfRemnant(this.remnant, pm.outline, x, y);
  };
  /* Rose Gold stock that is already cut away is tested against the whole
     silhouette, not only the eroded mask. Erosion can erase a thin part such
     as a jump ring entirely, and no pad around the removed stock stands in
     for a part the mask no longer has. front[i] is how many cells of line i
     are removed (with the pad); outline holds the silhouette's first filled
     cell on each row and column, in the eroded mask's frame.               */
  const NO_CELL = 0x3fffffff;
  function remnantFront(profile, res, pad, W, H) {
    const n = profile.axis === "x" ? H : W, front = new Int32Array(n);
    for (let i = 0; i < n; i++) { const f = Rose.frontier(profile, i / res, (i + 1) / res, pad); front[i] = f > 0 ? Math.ceil(f * res) : 0; }
    return { axis: profile.axis, front };
  }
  function outlineEdges(bits, w, h, off) {
    const rows = new Int32Array(h).fill(NO_CELL), cols = new Int32Array(w).fill(NO_CELL);
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) if (bits[y * w + x]) { if (rows[y] === NO_CELL) rows[y] = x; if (cols[x] === NO_CELL) cols[x] = y; }
    return { off, rows, cols };
  }
  // The eroded mask already keeps an ordinary edge `pad` from the removed stock, so its silhouette stays `pad` less the erosion away.
  function outlinePad(pad, erodeFine, res) { return Math.max(0, pad - erodeFine / res); }
  function fineMask(eroded, rot, erodeFine) { const pm = packShifted(eroded.bits, eroded.w, eroded.h); if (erodeFine) pm.outline = outlineEdges(rot.bits, rot.w, rot.h, -erodeFine); return pm; }
  function clearOfRemnant(rem, u, x, y) {
    const f = rem.front, n = f.length, ox = x + u.off, oy = y + u.off;
    if (rem.axis === "x") { for (let r = 0; r < u.rows.length; r++) { const g = oy + r; if (g >= 0 && g < n && ox + u.rows[r] < f[g]) return false; } }
    else for (let c = 0; c < u.cols.length; c++) { const g = ox + c; if (g >= 0 && g < n && oy + u.cols[c] < f[g]) return false; }
    return true;
  }
  /** Count overlapping cells (used for coarse tolerance + contact scoring). */
  Grid.prototype.overlap = function (pm, x, y, limit) {
    const v = pm.variants[x & 31], wx = x >> 5, gw = this.words, mw = pm.words, occ = this.occ;
    let n = 0;
    for (let r = 0; r < pm.h; r++) {
      const gy = y + r; if (gy < 0 || gy >= this.H) { if (!this.materialOnly) n += pm.cells; if (n > limit) return n; continue; }
      const gi = gy * gw + wx, mi = r * mw;
      for (let k = 0; k < mw; k++) {
        const gk = wx + k; const g = (gk < 0 || gk >= gw) ? (this.materialOnly ? 0 : 0xFFFFFFFF) : occ[gi + k];
        const a = g & v[mi + k]; if (a) { n += popcount32(a); if (n > limit) return n; }
      }
    }
    return n;
  };
  Grid.prototype.stamp = function (bits, w, h, x, y, id) {
    for (let r = 0; r < h; r++) for (let c = 0; c < w; c++) if (bits[r * w + c]) this.set(x + c, y + r);
    if (this.parts) { const grid = new Grid(w, h); grid.materialOnly = true; grid.stamp(bits, w, h, 0, 0); this.parts.push({x,y,w,h,grid,id}); }
  };
  /** Summed-area table of occupancy; cells outside the grid count as occupied. */
  Grid.prototype.buildSAT = function () {
    const W = this.W, H = this.H, S = this.sat || (this.sat = new Int32Array((W + 1) * (H + 1)));
    for (let y = 1; y <= H; y++) { let row = 0; for (let x = 1; x <= W; x++) { row += this.get(x - 1, y - 1); S[y * (W + 1) + x] = S[(y - 1) * (W + 1) + x] + row; } }
    if (this.material) this.material.buildSAT();
    return S;
  };
  Grid.prototype.boxSum = function (x0, y0, x1, y1) {   // half-open [x0,x1) × [y0,y1)
    const W = this.W, H = this.H, S = this.sat;
    const cx0 = Math.max(0, x0), cy0 = Math.max(0, y0), cx1 = Math.min(W, x1), cy1 = Math.min(H, y1);
    let inside = 0;
    if (cx1 > cx0 && cy1 > cy0) inside = S[cy1 * (W + 1) + cx1] - S[cy0 * (W + 1) + cx1] - S[cy1 * (W + 1) + cx0] + S[cy0 * (W + 1) + cx0];
    const total = (x1 - x0) * (y1 - y0), clipped = Math.max(0, cx1 - cx0) * Math.max(0, cy1 - cy0);
    return inside + (this.materialOnly ? 0 : total - clipped); // only the collision grid treats outside as wall
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
  // Search records own every rotation mask. Only layout data may leave the
  // solver: cloning the records for previews/checkpoints multiplies that memory.
  function publicLayout(layout) {
    if (!layout) return layout;
    const { grids, rec, ...out } = layout;
    out.placements = (layout.placements || []).map(p => ({ ...p }));
    for (const key of ["rejects", "capped", "liftedForOrders"]) if (Array.isArray(layout[key])) out[key] = layout[key].slice();
    if (layout.pocket) out.pocket = { ...layout.pocket };
    if (layout.params) out.params = { ...layout.params, ...(layout.params.angles ? { angles: layout.params.angles.slice() } : {}), ...(layout.params.pieceOrder ? { pieceOrder: layout.params.pieceOrder.slice() } : {}) };
    return out;
  }
  function bestResult(result, incumbent, sheet) {
    if (!incumbent || result && betterLayout(result, incumbent, sheet)) return publicLayout(result);
    const out = { ...publicLayout(result), ...publicLayout(incumbent), retainedBest: true };
    for (const key of ["endedBy", "trials", "elapsedMs"]) if (result?.[key] != null) out[key] = result[key];
    if (result?.params) out.params = { ...result.params, ...incumbent.params };
    return out;
  }
  const SHAPE_FAMILIES = ["round", "compact", "elongated", "concave", "branched", "angular"];
  function normalizePackingPlan(plan, count) {
    const valid = n => Number.isInteger(n) && n >= 0 && n < count;
    const score = n => Number.isFinite(n) ? Math.max(0, Math.min(100, n)) : 0;
    const angles = xs => (Array.isArray(xs) ? xs : []).filter(Number.isFinite).slice(0,3).map(a=>((Math.round(a)%360)+360)%360);
    const seen = new Set(), paired = new Set();
    const profiles = (Array.isArray(plan.profiles) ? plan.profiles : []).filter(p=>p && valid(p.index) && !seen.has(p.index) && seen.add(p.index) && [p.adaptability,p.interlock,p.edgeAffinity,p.priority].every(Number.isFinite) && SHAPE_FAMILIES.includes(p.family)).map(p=>({index:p.index,adaptability:score(p.adaptability),interlock:score(p.interlock),edgeAffinity:score(p.edgeAffinity),priority:score(p.priority),family:p.family,edgeRole:["long-edge","short-edge","corner","interior","either"].includes(p.edgeRole)?p.edgeRole:"either",mates:(Array.isArray(p.mates)?p.mates:[]).filter(x=>SHAPE_FAMILIES.includes(x)).slice(0,6),angles:angles(p.angles),note:String(p.note||"").slice(0,180)}));
    const pairs = (Array.isArray(plan.pairs) ? plan.pairs : []).filter(p=>{if(!p || !valid(p.a) || !valid(p.b) || p.a===p.b || !Number.isFinite(p.score))return false;const k=[p.a,p.b].sort((a,b)=>a-b).join(":");if(paired.has(k))return false;paired.add(k);return true;}).slice(0,240).map(p=>({a:p.a,b:p.b,score:score(p.score),reason:String(p.reason||"").slice(0,160)}));
    const hintsSeen=new Set();
    const suggestions=(Array.isArray(plan.suggestions)?plan.suggestions:[]).filter(p=>p && valid(p.index) && !hintsSeen.has(p.index) && hintsSeen.add(p.index)).slice(0,24).map(p=>({index:p.index,angles:angles(p.angles)}));
    return {profiles,pairs,suggestions,summary:String(plan.summary||"").slice(0,700)};
  }
  function shapeKey(p) { return JSON.stringify([p.hash || p.id,p.w,p.h,p.scale,p.areaPt2]); }
  function packingCompatibility(a, b, hints) {
    const explicit=hints?.partners?.[a]?.[b];
    if(Number.isFinite(explicit))return Math.max(0,Math.min(1,explicit/100));
    const x=hints?.profiles?.[a],y=hints?.profiles?.[b];
    if(!x || !y)return 0;
    // Family matches are model guidance too, including shapes from different batches.
    return ((x.mates?.includes(y.family)?1:0)+(y.mates?.includes(x.family)?1:0))*.25 * Math.min(x.interlock,y.interlock)/100;
  }
  function guidedOrderScore(p, placed, hints) {
    const profile=hints?.profiles?.[p.id];
    if(!profile)return 0;
    const mate=placed.reduce((n,r)=>Math.max(n,packingCompatibility(p.id,r.p?.id || r.id,hints)),0);
    return profile.priority + .25*(100-profile.adaptability) + .15*profile.interlock + 100*mate;
  }
  // Append searches treat saved placements as obstacles, and Rose Gold
  // contours as reserved material. Every streamed layout retains the originals.
  async function solveAppend(job, cb) {
    const guard=job.protectedRose, fixed=job.lockedPlacements?.length?job.lockedPlacements:guard.placements, ids=new Set(fixed.map(p=>p.id));
    if(guard)Rose.validate(guard.profile,job.sheet.wPt,job.sheet.hPt);
    if(ids.size!==fixed.length || fixed.some(p=>!job.pieces.some(c=>c.id===p.id)))throw new Error('Reload the saved charms before appending to this sheet');
    const usable=makeSheetGrid(job.sheet,job.clearancePt,job.fineRes||2).usableCells/Math.pow(job.fineRes||2,2);
    const remainderSheet={...job.sheet,...(guard?{remnant:guard.profile}:{}),fixedPieces:fixed.map(placement=>({piece:job.pieces.find(p=>p.id===placement.id),placement}))};
    const remaining=makeSheetGrid(remainderSheet,job.clearancePt,job.fineRes||2).usableCells/Math.pow(job.fineRes||2,2);
    const fixedArea=fixed.reduce((n,p)=>{const v=prepareVariant(job.pieces.find(c=>c.id===p.id),p.angle,job.clearancePt,job.fineRes||2);return n+(v?areaOf(v.fine.bits)/Math.pow(job.fineRes||2,2):0);},0);
    const cap=Math.max(0,usable*(job.maxFill||.8)-fixedArea), pieces=job.pieces.filter(p=>!ids.has(p.id));
    const merge=r=>({...r,placements:[...fixed.map(p=>({...p})),...(r.placements||[])],placedPt2:fixedArea+(r.placedPt2||0),usablePt2:usable,freePt2:r.freePt2??remaining,placedCells:(fixedArea+(r.placedPt2||0))*Math.pow(job.fineRes||2,2),density:(fixedArea+(r.placedPt2||0))/Math.max(1,usable),params:{seed:job.seed,angles:job.angles,clearancePt:job.clearancePt,insetPt:job.sheet.insetPt,...r.params,maxFill:job.maxFill||.8}});
    const info=r=>({...r,placed:(r.placed||0)+fixed.length,total:job.pieces.length,...(r.placedPt2!=null?{placedPt2:r.placedPt2+fixedArea,usablePt2:usable}:{}),...(r.density!=null?{density:(r.density*remaining+fixedArea)/Math.max(1,usable)}:{})});
    if(!pieces.length || cap<=0 || remaining<=0)return merge({placements:[],rejects:pieces.map(p=>p.id),trials:0,elapsedMs:0,endedBy:'cap',params:{maxFill:job.maxFill||.8}});
    const next={...job,protectedRose:null,lockedPlacements:null,sheet:remainderSheet,pieces,maxFill:Math.min(1,cap/remaining),initialLayout:(job.initialLayout||[]).filter(p=>!ids.has(p.id))};
    const result=await solve(next,{...cb,onPlaced:(p,i)=>cb.onPlaced?.(p,info(i)),onTrial:i=>cb.onTrial?.(info(i)),onBest:(r,i)=>cb.onBest?.(merge(r),info(i))});
    return merge(result);
  }
  async function solve(job, cb) {
    if(job.protectedRose||job.lockedPlacements?.length)return solveAppend(job,cb||{});
    cb = cb || {};
    const t0 = now();
    const metrics={layouts:0,searches:0,positions:0,gpuPositions:0,groups:0,branches:0,pruned:0};
    let lastMetricsAt=-Infinity,gpu=cb.gpu || null,gpuSamples=[],gpuCalls=0;
    const reportMetrics=(force=false)=>{if(force||now()-lastMetricsAt>=150){lastMetricsAt=now();cb.onMetrics?.({...metrics});}};
    const measuredSearch=async(...args)=>{
      let result;
      if(gpu && args[0].variants.length && args[2].W*args[2].H*args[0].variants.length>=20000 && !stopped()){
        // Explicit GPU selection persists; adaptive callers may still calibrate.
        const probe=!job.useGPU && !job.gpuBenchmark && (gpuSamples.length<3 || ++gpuCalls%40===0), gpuStart=now();
        try{
          const candidates=await gpu.candidates(args,solverAPI,(job.seed||1)+metrics.searches);
          const tested=args[0].variants.reduce((n,v)=>n+Math.max(0,args[2].W-v.coarse.w+1)*Math.max(0,args[2].H-v.coarse.h+1),0);
          metrics.positions+=tested;metrics.gpuPositions+=tested;
          if(stopped()){reportMetrics(true);return null;}
          const gpuArgs=args.slice();gpuArgs[12]=candidates;gpuArgs[13]=metrics;gpuArgs[14]=reportMetrics;
          result=search(...gpuArgs);const gpuMs=now()-gpuStart;
          if(probe){
            const cpuArgs=args.slice();cpuArgs[6]=rng((job.seed||1)+metrics.searches);cpuArgs[13]=metrics;cpuArgs[14]=reportMetrics;
            const cpuStart=now(),baseline=search(...cpuArgs),cpuMs=now()-cpuStart;
            gpuSamples.push({gpuMs,cpuMs});
            if(baseline&&(!result||baseline.score>result.score))result=baseline;
            const samples=gpuSamples.slice(-3),gpuTime=samples.reduce((n,s)=>n+s.gpuMs,0),cpuTime=samples.reduce((n,s)=>n+s.cpuMs,0);
            if(samples.length===3){
              const faster=gpuTime<cpuTime*.9;
              cb.onGPU?.({active:faster,reason:faster?'GPU accelerated':'CPU is faster on this device',speedup:cpuTime/Math.max(.01,gpuTime),samples:samples.length});
              if(!faster && !job.gpuBenchmark){gpu.destroy?.();gpu=null;}
            }
          }
        }catch(e){gpu?.destroy?.();gpu=null;cb.onGPU?.({active:false,reason:'CPU fallback: '+String(e.message||e)});}
      }
      if(result===undefined){const cpuArgs=args.slice();cpuArgs[13]=metrics;cpuArgs[14]=reportMetrics;result=search(...cpuArgs);}
      metrics.searches++;reportMetrics();return result;
    };
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
    const stallMs = job.fullBudget ? 0 : +job.stallMs || 0; let lastBetterAt = now(), stalled = false; let best = null;
    const stopped = () => (cb.shouldStop && cb.shouldStop()) || (now() - t0) > budget || (stalled = !!(stallMs && !job.packingPending && best && (!best.rejects.length || best.rejects.every(id => best.capped?.includes(id))) && (now() - lastBetterAt) > stallMs));

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
    if(job.sheet.remnant){
      const pad=Math.max(insetPt,halfGapFine/fineRes);
      Rose.stamp(baseFine,job.sheet.remnant,fineRes,pad);
      Rose.stamp(baseCoarse,job.sheet.remnant,coarseRes,pad);
      baseFine.remnant=remnantFront(job.sheet.remnant,fineRes,outlinePad(pad,erodeFine,fineRes),FW,FH);
    }
    stampFixed(baseFine,job.sheet.fixedPieces,clearancePt,fineRes);
    stampFixed(baseCoarse,job.sheet.fixedPieces,clearancePt,coarseRes);
    baseCoarse.buildSAT();
    const usableCellsFine = baseFine.freeCells();
    // Walls constrain placement but are never counted as neighbouring charms.
    baseFine.trackMaterial(true); baseCoarse.trackMaterial(); baseCoarse.material.buildSAT();

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
          fine: { bits: dil.bits, w: dil.w, h: dil.h, pm: fineMask(dil, rot, erodeFine) }, cells: areaOf(dil.bits),
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
    /* ── partial Rose Gold sheet: the next green line's envelope ───────────
       The contour keeps, on each line of its axis, everything up to the
       farthest charm. frontier[i] is where free stock starts on line i now;
       an envelope adds each placed mask's far edge. Its area is the stock the
       next green line would remove, which layouts on such sheets minimise
       once the queue is known to leave room (fitLine, set below).            */
    const roseAxis = job.sheet.remnant ? job.sheet.remnant.axis : null;
    const frontier = roseAxis ? (() => { const n = roseAxis === "x" ? FH : FW, D = roseAxis === "x" ? FW : FH, f = new Int32Array(n); for (let i = 0; i < n; i++) { let d = 0; while (d < D && (roseAxis === "x" ? baseFine.get(d, i) : baseFine.get(i, d))) d++; f[i] = d; } return f; })() : null;
    const envelopeOf = rec => { const E = Int32Array.from(frontier); for (const r of rec) { const far = lineProfile(r.v.fine, roseAxis), base = roseAxis === "x" ? r.y : r.x, off = roseAxis === "x" ? r.x : r.y; for (let j = 0; j < far.length; j++) if (far[j] >= 0 && off + far[j] + 1 > E[base + j]) E[base + j] = off + far[j] + 1; } return E; };
    const envelopePt2 = rec => { if (!fitLine || !rec) return undefined; const E = envelopeOf(rec); let a = 0; for (let i = 0; i < E.length; i++) a += E[i] - frontier[i]; return a / (fineRes * fineRes); };
    const envelopeStrip = rec => { const fine = envelopeOf(rec), coarse = new Int32Array(Math.ceil(fine.length / ratio)); for (let i = 0; i < fine.length; i++) { const c = Math.floor(i / ratio), d = Math.ceil(fine[i] / ratio); if (d > coarse[c]) coarse[c] = d; } return { axis: roseAxis, fine, coarse }; };
    const withEnvelope = layout => layout && layout.rec ? { ...layout, envelopePt2: envelopePt2(layout.rec) } : layout;
    // Every published best carries its envelope, so parallel workers compare alike.
    if (roseAxis && cb.onBest) { const publish = cb.onBest; cb = { ...cb, onBest: (layout, info) => publish(fitLine && layout && best?.rec && layout.placements?.length === best.rec.length ? { ...layout, envelopePt2: envelopePt2(best.rec) } : layout, info) }; }
    let fitLine = false, fitNow = false;
    const stripAxis = FW >= FH ? "x" : "y";
    let stripWeight = 4;
    const stripOf = rec => ({ weight:stripWeight, axis: stripAxis, end: rec.reduce((n, r) => Math.max(n, stripAxis === "x" ? r.x + r.v.fine.w : r.y + r.v.fine.h), wallFine), ...(fitNow ? { envelope: envelopeStrip(rec) } : {}) });
    const qualityOf = (rec, fine) => rec.length ? rec.reduce((n, r) => n + placementAt(r.v, fine, r.x, r.y).score, 0) / rec.length : 0;

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
          variants.push({ angle: a, fine: { bits: dil.bits, w: dil.w, h: dil.h, pm: fineMask(dil, rot, erodeFine) }, cells: areaOf(dil.bits), solid: { bits: rot.bits, w: rot.w, h: rot.h, cx: rot.cx + halfGapFine - erodeFine, cy: rot.cy + halfGapFine - erodeFine }, ringFine: packShifted(rg.bits, rg.w, rg.h), ringPad: Math.max(2, Math.round(2 * fineRes)), coarse: { pm: packShifted(co.bits, co.w, co.h), w: co.w, h: co.h } });
        }
        const pos = await measuredSearch({ variants }, fine, coarse, ratio, 0.35, 0, random, 0, 0, best.stripPacked ? stripOf(best.rec) : null);
        if (!pos) continue;
        const { v, x, y } = pos;
        if ((best.placedCells + v.cells) / usableCellsFine > maxFill) continue;
        fine.stamp(v.fine.bits, v.fine.w, v.fine.h, x, y, p.id);
        for (let r = 0; r < v.fine.h; r++) for (let c = 0; c < v.fine.w; c++) if (v.fine.bits[r * v.fine.w + c]) { const gx = Math.floor((x + c) / ratio), gy = Math.floor((y + r) / ratio); if (gx >= 0 && gy >= 0 && gx < coarse.W && gy < coarse.H) coarse.set(gx, gy); }
        coarse.buildSAT();
        const pl = { id, angle: v.angle, cxPt: (x + v.solid.cx) / fineRes, cyPt: (y + v.solid.cy) / fineRes, xPt: (x + (v.fine.w - v.solid.w) / 2) / fineRes, yPt: (y + (v.fine.h - v.solid.h) / 2) / fineRes, wPt: v.solid.w / fineRes, hPt: v.solid.h / fineRes, contact: pos.contact || 0, finishing: true };
        best.rec.push({ p, v, x, y });
        best.placements.push(pl); best.rejects = best.rejects.filter(r => r !== id);
        best.placedCells += v.cells; best.placedPt2 = best.placedCells / (fineRes * fineRes);
        best.density = best.placedCells / usableCellsFine; best.contactQuality = qualityOf(best.rec, fine); best.freePt2 = fine.freeCells() / (fineRes * fineRes); best.pocket = pocketPt(coarse, coarseRes);
        if (cb.onPlaced) cb.onPlaced(pl, { trial: best.trial, placed: best.placements.length, total, freePt2: best.freePt2, usablePt2: usableCellsFine / (fineRes * fineRes), placedPt2: best.placedPt2, pocket: best.pocket, finishing: true });
        if (cb.onBest) cb.onBest(publicLayout(best), { trial: best.trial, placed: best.placements.length, total, rejects: best.rejects, density: best.density, elapsedMs: now() - t0, finishing: true });
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
            p._v15.set(a, { angle: a, fine: { bits: dil.bits, w: dil.w, h: dil.h, pm: fineMask(dil, rot, erodeFine) }, cells: areaOf(dil.bits), solid: { bits: rot.bits, w: rot.w, h: rot.h, cx: rot.cx + halfGapFine - erodeFine, cy: rot.cy + halfGapFine - erodeFine }, ringFine: packShifted(rg.bits, rg.w, rg.h), ringPad: Math.max(2, Math.round(2 * fineRes)), coarse: { pm: packShifted(co.bits, co.w, co.h), w: co.w, h: co.h } });
          }
          const v = p._v15.get(a); if (v) out.push(v);
        }
        return out;
      };

    // Grow a winning FIFO layout in place, one whole oldest order at a time.
    // A failed group rolls back locally; it never replaces the saved winner.
    async function extendOldest(best) {
      if (!fifo || best.extended || !best.rec?.length) return false;
      best.extended = true; let improved = false;
      while (!stopped()) {
        const missing = repairCandidates(best).map(id => prepared.find(p => p.id === id));
        if (!missing.length || missing.some(p => !p || p.pinned) || (best.placedCells + missing.reduce((n,p)=>n+p.footprintCells,0))/usableCellsFine > maxFill) break;
        const rec = best.rec.slice(), grids = rebuildGrids(rec); let cells = best.placedCells, ok = true;
        for (const p of missing.sort((a,b)=>guidedOrderScore(b,rec,job.packingHints)-guidedOrderScore(a,rec,job.packingHints) || b.areaPt2-a.areaPt2)) {
          if (stopped()) { ok = false; break; }
          const variants = variantsFor(p, [...new Set(angles.concat(Array.from({length:72},(_,i)=>i*5)))]);
          const pos = await measuredSearch({...p,variants}, grids.fine, grids.coarse, ratio, .35, 0, random, 0, 0, null, true);
          if (!pos || (cells+pos.v.cells)/usableCellsFine > maxFill) { ok = false; break; }
          rec.push({p,v:pos.v,x:pos.x,y:pos.y}); cells += pos.v.cells;
          Object.assign(grids, rebuildGrids(rec));
        }
        if (!ok) break;
        const placements = rec.map(r=>({id:r.p.id,angle:r.v.angle,cxPt:(r.x+r.v.solid.cx)/fineRes,cyPt:(r.y+r.v.solid.cy)/fineRes,xPt:(r.x+(r.v.fine.w-r.v.solid.w)/2)/fineRes,yPt:(r.y+(r.v.fine.h-r.v.solid.h)/2)/fineRes,wPt:r.v.solid.w/fineRes,hPt:r.v.solid.h/fineRes}));
        const ids = new Set(placements.map(p=>p.id));
        Object.assign(best,{placements,rec,grids,placedCells:cells,placedPt2:cells/(fineRes*fineRes),density:cells/usableCellsFine,freePt2:grids.fine.freeCells()/(fineRes*fineRes),pocket:pocketPt(grids.coarse,coarseRes),contactQuality:qualityOf(rec,grids.fine),rejects:prepared.filter(p=>!ids.has(p.id)).map(p=>p.id)});
        improved = true; lastBetterAt = now();
        cb.onBest?.(publicLayout(best),{trial:best.trial,placed:placements.length,total:prepared.length,rejects:best.rejects.slice(),density:best.density,elapsedMs:now()-t0,finishing:true});
        await yieldNow();
      }
      return improved;
    }

    async function ruinRecreate(best, attempts, perimeter = null) {
      let missingIds = repairCandidates(best);
      if(perimeter && !fifo && missingIds.length){
        const groups=new Map();
        for(const id of missingIds){const p=prepared.find(p=>p.id===id);if(p){if(!groups.has(p.order))groups.set(p.order,[]);groups.get(p.order).push(p);}}
        const eligible=[...groups.values()].filter(ps=>ps.length<=3 && !ps.some(p=>p.pinned)).sort((a,b)=>a.reduce((n,p)=>n+p.footprintCells,0)-b.reduce((n,p)=>n+p.footprintCells,0));
        missingIds=(eligible[0] || []).map(p=>p.id);
      }
      if (!best?.rec?.length || !missingIds.length || missingIds.length > 3) return false;
      const outOfTime=()=>stopped() || (perimeter && now()>=perimeter.deadline);
      const corners=perimeter ? cornerPockets(best.grids.coarse,Math.max(2,Math.round(12*72/25.4*coarseRes))) : null;
      const total = prepared.length;
      const repairAngles = perimeter ? angles : Array.from({length:24},(_,i)=>i*15);
      for (let att = 0; att < attempts; att++) {
        if (outOfTime()) return false;
        // Target the emptiest corners during the final pass; ordinary repairs
        // alternate between the largest gap and a random neighbourhood.
        const rec = best.rec;
        let anchor;
        const corner=corners?.[att%corners.length];
        if(corner)anchor={x:corner.x*ratio,y:corner.y*ratio};
        else if (att % 2 === 0) { const pk = best.grids.coarse.largestPocket(); anchor = { x: (pk.x + pk.w / 2) * ratio, y: (pk.y + pk.h / 2) * ratio }; }
        else { const r = rec[Math.floor(random() * rec.length)]; anchor = { x: r.x + r.v.fine.w / 2, y: r.y + r.v.fine.h / 2 }; }
        const k = perimeter ? 2+Math.floor(att/4) : 2 + Math.floor(random() * 3); // bounded corner neighbourhoods
        const byDist = rec.slice().sort((a, b) => Math.hypot(a.x + a.v.fine.w / 2 - anchor.x, a.y + a.v.fine.h / 2 - anchor.y) - Math.hypot(b.x + b.v.fine.w / 2 - anchor.x, b.y + b.v.fine.h / 2 - anchor.y));
        const removed = perimeter ? byDist.filter(r=>!r.p.pinned).slice(0,k) : byDist.slice(0, k).filter(r => !r.p.pinned);
        if(!removed.length)continue;
        const keep = rec.filter(r => !removed.includes(r));
        // rebuild grids from the kept pieces
        const fine = baseFine.clone(), coarse = baseCoarse.clone();
        for (const r of keep) { fine.stamp(r.v.fine.bits, r.v.fine.w, r.v.fine.h, r.x, r.y, r.p.id); for (let yy = 0; yy < r.v.fine.h; yy++) for (let xx = 0; xx < r.v.fine.w; xx++) if (r.v.fine.bits[yy * r.v.fine.w + xx]) { const gx = Math.floor((r.x + xx) / ratio), gy = Math.floor((r.y + yy) / ratio); if (gx >= 0 && gy >= 0 && gx < coarse.W && gy < coarse.H) coarse.set(gx, gy); } }
        coarse.buildSAT();
        // Reinsert the complete missing order before its removed neighbours.
        // Corner repair uses the normal rotation set and shape guidance.
        const missing = missingIds.map(id => prepared.find(x => x.id === id)).filter(Boolean);
        if (missing.some(p => p.pinned)) continue;
        const queue = missing.concat(removed.map(r => r.p).sort((a, b) => guidedOrderScore(b,keep,job.packingHints)-guidedOrderScore(a,keep,job.packingHints) || b.areaPt2 - a.areaPt2));
        const newRec = keep.slice(); let ok = true, cells = keep.reduce((n,r) => n + r.v.cells, 0);
        for (const p of queue) {
          if (outOfTime()) { ok = false; break; }
          const pos = await measuredSearch({ ...p, variants: variantsFor(p, repairAngles) }, fine, coarse, ratio, 0.35, perimeter ? 0 : 0.02, random, corner?.right || 0, corner?.bottom || 0, best.stripPacked ? stripOf(newRec) : null);
          if (!pos) { ok = false; break; }
          const { v, x, y } = pos;
          if ((cells + v.cells) / usableCellsFine > maxFill) { ok = false; break; }
          cells += v.cells;
          fine.stamp(v.fine.bits, v.fine.w, v.fine.h, x, y, p.id);
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
        if(!betterLayout({placements,density:placedCells/usableCellsFine,contactQuality:qualityOf(newRec,fine),envelopePt2:envelopePt2(newRec)},withEnvelope(best),job.sheet))continue;
        Object.assign(best, { placements, rejects, capped: (best.capped || []).filter(id => !placedIds.has(id)), density: placedCells / usableCellsFine, placedCells, placedPt2: placedCells / (fineRes * fineRes), freePt2: fine.freeCells() / (fineRes * fineRes), pocket: pocketPt(coarse, coarseRes), grids: { fine, coarse }, rec: newRec, contactQuality:qualityOf(newRec,fine), repaired: true });
        if(perimeter)best.cornerRepairs=(best.cornerRepairs || 0)+1;
        if (cb.onBest) cb.onBest(publicLayout(best), { trial: best.trial, placed: placements.length, total, rejects, density: best.density, elapsedMs: now() - t0, repaired: true, removed: removed.length });
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
    // A partial sheet that the admitted queue leaves room on follows its green
    // line. A queue that fills the sheet keeps the plain search and ranking.
    fitLine = !!roseAxis && prepared.filter(p => !fifo || rank.get(p.order) < capOrders).reduce((n, p) => n + p.footprintCells, 0) / usableCellsFine < maxFill * 0.9;
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
      for (const r of rec) { fine.stamp(r.v.fine.bits, r.v.fine.w, r.v.fine.h, r.x, r.y, r.p.id); for (let yy = 0; yy < r.v.fine.h; yy++) for (let xx = 0; xx < r.v.fine.w; xx++) if (r.v.fine.bits[yy * r.v.fine.w + xx]) { const gx = Math.floor((r.x + xx) / ratio), gy = Math.floor((r.y + yy) / ratio); if (gx >= 0 && gy >= 0 && gx < coarse.W && gy < coarse.H) coarse.set(gx, gy); } }
      coarse.buildSAT(); return { fine, coarse };
    };
    // A re-nest may start from an existing layout, but only after rebuilding and
    // validating it against this job's current shapes, pins, dates and fill cap.
    if (job.initialLayout?.length && prepared.length === pieces.length) {
      const rec = [], ids = new Set(); let valid = true;
      for (const pl of job.initialLayout) {
        const p = prepared.find(p => p.id === pl.id);
        if (!p || ids.has(pl.id) || ![pl.cxPt,pl.cyPt,pl.angle].every(Number.isFinite)) { valid = false; break; }
        if (p.pinned && (pl.cxPt !== p.pinned.cxPt || pl.cyPt !== p.pinned.cyPt || pl.angle !== p.pinned.angle)) { valid = false; break; }
        const v = variantsFor(p, [((pl.angle % 360) + 360) % 360])[0];
        if (!v) { valid = false; break; }
        const x = Math.round(pl.cxPt * fineRes - v.solid.cx), y = Math.round(pl.cyPt * fineRes - v.solid.cy);
        const grids = rebuildGrids(rec);
        if (!grids.fine.fits(v.fine.pm,x,y)) { valid = false; break; }
        ids.add(p.id); rec.push({p,v,x,y});
      }
      const rejects = prepared.filter(p => !ids.has(p.id));
      const cells = rec.reduce((n,r) => n+r.v.cells,0);
      const cutoff = fifo && rejects.length ? Math.min(...rejects.map(p=>rank.get(p.order))) : Infinity;
      if (prepared.some(p=>p.pinned && !ids.has(p.id)) || rec.some(r => rank.get(r.p.order) >= cutoff || rejects.some(p=>p.order===r.p.order)) || cells/usableCellsFine > maxFill) valid = false;
      if (valid && rec.length) {
        const grids = rebuildGrids(rec), placements = rec.map(r=>({id:r.p.id,angle:r.v.angle,cxPt:(r.x+r.v.solid.cx)/fineRes,cyPt:(r.y+r.v.solid.cy)/fineRes,xPt:(r.x+(r.v.fine.w-r.v.solid.w)/2)/fineRes,yPt:(r.y+(r.v.fine.h-r.v.solid.h)/2)/fineRes,wPt:r.v.solid.w/fineRes,hPt:r.v.solid.h/fineRes}));
        best = {placements,rejects:rejects.map(p=>p.id),capped:rejects.filter(p=>fifo && rank.get(p.order)>=capOrders).map(p=>p.id),density:cells/usableCellsFine,contactQuality:qualityOf(rec,grids.fine),trial:-1,stripPacked:cells/usableCellsFine<maxFill*.9,usablePt2:usableCellsFine/(fineRes*fineRes),freePt2:grids.fine.freeCells()/(fineRes*fineRes),placedPt2:cells/(fineRes*fineRes),placedCells:cells,pocket:pocketPt(grids.coarse,coarseRes),grids,rec,retainedInitial:true};
        if (cb.onBest) cb.onBest(publicLayout(best),{trial:-1,placed:placements.length,total:prepared.length,rejects:best.rejects,density:best.density,elapsedMs:now()-t0});
      }
    }
    /* ── careful append ─────────────────────────────────────────────────────
       A few new charms joining a saved sheet (the live Gold and Silver flow). Restarts of the whole construction
       rarely change anything there, and each one pushed the charm into the leftmost gap it fitted, however poorly,
       leaving slivers no later charm could use (the lotus and the sausage dog, 23 Sep). Here the charm's resting
       spots in every gap it fits, at every angle, are graded once (see spotsFor: tight slots and voids far back
       included, 24 Sep), and the charm goes where it leaves the least unusable space:
         waste   what the free space around it loses beyond the charm's own area. Free space is worth the share of
                 the shop's charms that fit the pocket it lies in (see PROBES): an open area is worth all of it, a
                 sliver nothing. So a charm that closes off a pocket most charms could still use pays for it, and one
                 that fills a pocket few could use gains (the dead pockets and the feathers, 24 Sep),
         around  the empty space just outside its outline: each step along the outline adds the free distance to the
                 nearest charm or edge, up to AROUND_MM, so a charm touching its neighbours all round scores near nothing,
         growth  how far it widens the occupied part of the sheet, and a little for lying further along it.
       The best spots are then nudged: slid left against their neighbour, then up or down to the nearer charm or
       edge, and kept there when that grades no worse. Charms join best fit first. One that fits nowhere is set aside
       with its order and reported at once (see pass). cb.onProbe reports the spot being graded, so the page can draw
       the charm turning and moving while it searches.                                                           */
    const carefulOn = !!job.careful && !!job.sheet.fixedPieces?.length && !roseAxis;
    async function carefulAppend() {
      const admitted = fifo ? prepared.filter(p => rank.get(p.order) < capOrders) : prepared.slice();
      if (admitted.some(p => p.pinned)) return null;
      const MM_PX = fineRes * 72 / 25.4, cellMm2 = 1 / (MM_PX * MM_PX);
      const sliverMm = +job.sliverMm || SLIVER_MM, weights = { ...FIT_WEIGHTS, ...(job.fitWeights || {}) };
      const rPx = sliverMm * MM_PX / 2, rU = Math.round(3 * rPx), reach = Math.ceil(2 * rPx) + 2, margin = Math.max(reach, Math.ceil(POCKET_MM * MM_PX));
      const aroundU = Math.round(3 * (+job.aroundMm || AROUND_MM) * MM_PX);
      // the second search takes the angles halfway between the first one's, so the two together try twice as many
      const turn = job.exploreRotations && angles.length > 1 ? 180 / angles.length : 0;
      for (const p of admitted) p.careful = variantsFor(p, angles.map(a => (a + turn) % 360));
      const FW = baseFine.W, FH = baseFine.H, axisX = stripAxis === "x";
      let fine = baseFine.clone(), coarse = baseCoarse.clone();
      let rec = [], cells = 0, lastProbe = -Infinity, graded = 0;
      let lastStage = null;
      // at most one probe every 40 ms, but always the first of each stage, so every step of the search is shown
      const probe = (p, v, x, y, stage, force) => {
        const t = now(); if (!cb.onProbe || (!force && stage === lastStage && t - lastProbe < 40)) return; lastProbe = t; lastStage = stage;
        cb.onProbe({ id: p.id, angle: v.angle, cxPt: (x + v.solid.cx) / fineRes, cyPt: (y + v.solid.cy) / fineRes, stage, graded });
      };
      // what the free space on the whole sheet is worth before this charm (see PROBES), summed so any window reads it at once
      const all = { occ: new Uint8Array(FW * FH), A: new Int32Array(FW * FH), B: new Int32Array(FW * FH), out: new Uint8Array(FW * FH), label: new Int32Array(FW * FH), stack: new Int32Array(FW * FH), val: new Float32Array(FW * FH), regions: [] }, valSAT = new Float64Array((FW + 1) * (FH + 1));
      const probes = probeVariants(fineRes, erodeFine, halfGapFine), padCells = Math.round(MM_PX), openU = OPEN_MM * MM_PX * 1.5;
      const valueIn = (x0, y0, x1, y1) => { x0 = Math.max(0, x0); y0 = Math.max(0, y0); x1 = Math.min(FW, x1); y1 = Math.min(FH, y1); if (x1 <= x0 || y1 <= y0) return 0; const W1 = FW + 1; return valSAT[y1 * W1 + x1] - valSAT[y0 * W1 + x1] - valSAT[y1 * W1 + x0] + valSAT[y0 * W1 + x0]; };
      /** Label the regions of free cells a SLIVER_MM disc sweeps (4-connected) inside [x0, x1) × [y0, y1) of a domain;
          each gets its size, box, widest free circle (in chamfer units) and whether it runs into the edge of the box. */
      const regionsOf = (occ, out, A, label, stack, W, x0, y0, x1, y1, visit) => {
        const regions = [];
        for (let y = y0; y < y1; y++) for (let x = x0; x < x1; x++) {
          const i0 = y * W + x; if (occ[i0] || out[i0] || label[i0] >= 0) continue;
          const id = regions.length, r = { size: 0, bb: [x, y, x + 1, y + 1], maxA: 0, edge: false, kept: 0, same: -2 };
          let sp = 0; stack[sp++] = i0; label[i0] = id;
          while (sp) {
            const i = stack[--sp], cx = i % W, cy = (i - cx) / W;
            r.size++; if (A[i] > r.maxA) r.maxA = A[i];
            if (cx < r.bb[0]) r.bb[0] = cx; if (cx >= r.bb[2]) r.bb[2] = cx + 1; if (cy < r.bb[1]) r.bb[1] = cy; if (cy >= r.bb[3]) r.bb[3] = cy + 1;
            if (cx === x0 || cy === y0 || cx === x1 - 1 || cy === y1 - 1) r.edge = true;
            if (visit) visit(r, i);
            if (cx > x0 && !occ[i - 1] && !out[i - 1] && label[i - 1] < 0) { label[i - 1] = id; stack[sp++] = i - 1; }
            if (cx < x1 - 1 && !occ[i + 1] && !out[i + 1] && label[i + 1] < 0) { label[i + 1] = id; stack[sp++] = i + 1; }
            if (cy > y0 && !occ[i - W] && !out[i - W] && label[i - W] < 0) { label[i - W] = id; stack[sp++] = i - W; }
            if (cy < y1 - 1 && !occ[i + W] && !out[i + W] && label[i + W] < 0) { label[i + W] = id; stack[sp++] = i + W; }
          }
          regions.push(r);
        }
        return regions;
      };
      const sheetValue = () => {
        for (let y = 0; y < FH; y++) for (let x = 0; x < FW; x++) all.occ[y * FW + x] = fine.get(x, y);
        sliverCells(all.occ, FW, FH, rU, all.A, all.B, all.out);
        all.label.fill(-1);
        all.regions = regionsOf(all.occ, all.out, all.A, all.label, all.stack, FW, 0, 0, FW, FH);
        const D = { W: FW, H: FH, occ: all.occ, label: all.label };
        for (let id = 0; id < all.regions.length; id++) { const r = all.regions[id]; r.share = r.maxA >= openU ? 1 : regionShare(D, id, r.bb, probes, padCells); }
        for (let i = 0; i < FW * FH; i++) all.val[i] = all.label[i] >= 0 ? all.regions[all.label[i]].share : 0;
        for (let y = 1; y <= FH; y++) { let row = 0; for (let x = 1; x <= FW; x++) { row += all.val[(y - 1) * FW + x - 1]; valSAT[y * (FW + 1) + x] = valSAT[(y - 1) * (FW + 1) + x] + row; } }
      };
      // how far the charms already reach along the sheet (the inset band is not a charm)
      const reachOf = () => {
        if (axisX) { for (let x = FW - wallFine - 1; x >= wallFine; x--) for (let y = wallFine; y < FH - wallFine; y++) if (fine.get(x, y)) return x + 1; }
        else { for (let y = FH - wallFine - 1; y >= wallFine; y--) for (let x = wallFine; x < FW - wallFine; x++) if (fine.get(x, y)) return y + 1; }
        return wallFine;
      };
      /** Occupied cells under a packed ring at (x, y); cells off the sheet count as occupied, each once. */
      const ringOccupied = (pm, x, y) => {
        const bits = pm.variants[x & 31], wx = x >> 5, gw = fine.words, mw = pm.words, g = fine.occ; let n = 0;
        for (let r = 0; r < pm.h; r++) {
          const gy = y + r, mi = r * mw;
          if (gy < 0 || gy >= fine.H) { for (let k = 0; k < mw; k++) n += popcount32(bits[mi + k]); continue; }
          const gi = gy * gw + wx;
          for (let k = 0; k < mw; k++) { const gk = wx + k, a = (gk < 0 || gk >= gw ? 0xFFFFFFFF : g[gi + k]) & bits[mi + k]; if (a) n += popcount32(a); }
        }
        return n;
      };
      let local = null;
      /** What a charm at (x, y) costs the free space around it beyond its own area, in cells: the worth of the space
          it covers and of the space it leaves in pockets no longer open to the rest, less its own area. Filling
          space few charms could use costs less than nothing; closing off a pocket costs what the pocket was worth. */
      const lossAt = (v, x, y) => {
        const cx0 = x - margin, cy0 = y - margin, cx1 = x + v.fine.w + margin, cy1 = y + v.fine.h + margin;
        const X0 = Math.max(0, cx0 - reach), Y0 = Math.max(0, cy0 - reach), X1 = Math.min(FW, cx1 + reach), Y1 = Math.min(FH, cy1 + reach);
        const W = X1 - X0, H = Y1 - Y0, n = W * H;
        if (!local || local.n < n) local = { n, occ: new Uint8Array(n), A: new Int32Array(n), B: new Int32Array(n), out: new Uint8Array(n), label: new Int32Array(n), stack: new Int32Array(n) };
        const { occ } = local, words = fine.words, g = fine.occ;
        for (let yy = 0; yy < H; yy++) { const row = (Y0 + yy) * words; for (let xx = 0, i = yy * W; xx < W; xx++, i++) { const gx = X0 + xx; occ[i] = (g[row + (gx >> 5)] >>> (gx & 31)) & 1; } }
        const bits = v.fine.bits, mw = v.fine.w;
        for (let r = 0; r < v.fine.h; r++) { const yy = y + r - Y0; if (yy < 0 || yy >= H) continue; for (let c = 0; c < mw; c++) if (bits[r * mw + c]) { const xx = x + c - X0; if (xx >= 0 && xx < W) occ[yy * W + xx] = 1; } }
        sliverCells(occ, W, H, rU, local.A, local.B, local.out);
        const ax0 = Math.max(cx0, X0) - X0, ay0 = Math.max(cy0, Y0) - Y0, ax1 = Math.min(cx1, X1) - X0, ay1 = Math.min(cy1, Y1) - Y0;
        local.label.fill(-1, 0, n);
        // regions that run out of the window are still part of the space beyond it and keep their worth cell by cell;
        // one inside it is a pocket: worth what it was if it is a region the sheet already had, else what PROBES say
        const regions = regionsOf(occ, local.out, local.A, local.label, local.stack, W, ax0, ay0, ax1, ay1, (r, i) => {
          const gi = (Y0 + ((i / W) | 0)) * FW + X0 + (i % W), gl = all.label[gi];
          r.kept += all.val[gi]; r.same = r.same === -2 ? gl : r.same === gl ? gl : -1;
        });
        let after = 0;
        const D = { W, H, occ, label: local.label };
        for (let id = 0; id < regions.length; id++) {
          const r = regions[id];
          if (r.edge) after += r.kept;
          else if (r.same >= 0 && all.regions[r.same].size === r.size) after += r.size * all.regions[r.same].share;
          else after += r.size * (r.maxA >= openU ? 1 : regionShare(D, id, r.bb, probes, padCells));
        }
        return valueIn(X0 + ax0, Y0 + ay0, X0 + ax1, Y0 + ay1) - after - v.cells;
      };
      /** Empty space around a charm at (x, y), in cells: the free distance beyond each outline cell to the nearest charm
          or edge on the sheet as it stands, up to aroundU. Off the sheet is the edge. */
      const aroundAt = (v, x, y) => {
        const e = v.outline || (v.outline = (() => { const r = ring(v.fine.bits, v.fine.w, v.fine.h, 1), at = []; for (let yy = 0; yy < r.h; yy++) for (let xx = 0; xx < r.w; xx++) if (r.bits[yy * r.w + xx]) at.push(xx - 1, yy - 1); return Int16Array.from(at); })());
        const A = all.A; let s = 0;
        for (let k = 0; k < e.length; k += 2) { const gx = x + e[k], gy = y + e[k + 1]; if (gx < 0 || gy < 0 || gx >= FW || gy >= FH) continue; const d = A[gy * FW + gx]; s += d < aroundU ? d : aroundU; }
        return s / 3;
      };
      /** The grade of a spot: higher is better, in mm² of space given up. */
      const grade = (v, x, y, front) => {
        const waste = lossAt(v, x, y) * cellMm2;
        const around = aroundAt(v, x, y) * cellMm2;
        const far = axisX ? x + v.fine.w : y + v.fine.h, growth = Math.max(0, far - front) / MM_PX;
        const along = (axisX ? x + v.fine.w / 2 : y + v.fine.h / 2) / MM_PX;
        graded++;
        return { waste, around, growth, fit: -(waste + weights.around * around + weights.growth * growth + weights.along * along) };
      };
      /* Where a charm can go, exactly. At each angle every position on the fine grid is legal or not; the positions
         are taken 4×4 at a time: a block is out when even the part of the charm all 16 positions share hits
         something, in when everything any of them covers is clear, and only the rest are tested one by one. So a
         spot the charm fits with no room to spare, which the coarse scan could not see, is found like any other.
         The legal positions form regions, one for each gap, pocket or open area the charm fits in at that angle.
         A region's corners (positions stopped both across and along the sheet) are where the charm rests against
         its neighbours; the snuggest few corners of every region are kept, so no gap, however far back or however
         tight, goes untried, and the snuggest corners at each angle are kept on top of those.                     */
      const cropped = (bits, W, H) => {
        let x0 = W, y0 = H, x1 = -1, y1 = -1;
        for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) if (bits[y * W + x]) { if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y; }
        if (x1 < 0) return null;
        const w = x1 - x0 + 1, h = y1 - y0 + 1, out = new Uint8Array(w * h);
        for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) out[y * w + x] = bits[(y + y0) * W + x + x0];
        return { pm: packShifted(out, w, h), x: x0, y: y0 };
      };
      /** What all positions of a ratio×ratio block cover (core) and what any of them covers (hull). */
      const blockMasks = v => {
        const w = v.fine.w, h = v.fine.h, b = v.fine.bits, B = ratio, W2 = w + B - 1, H2 = h + B - 1;
        const rAll = new Uint8Array(W2 * h), rAny = new Uint8Array(W2 * h), core = new Uint8Array(W2 * H2), hull = new Uint8Array(W2 * H2);
        for (let y = 0; y < h; y++) for (let x = 0; x < W2; x++) {
          let all = 1, any = 0;
          for (let o = 0; o < B; o++) { const mx = x - o; if (mx >= 0 && mx < w && b[y * w + mx]) any = 1; else all = 0; }
          rAll[y * W2 + x] = all; rAny[y * W2 + x] = any;
        }
        for (let y = 0; y < H2; y++) for (let x = 0; x < W2; x++) {
          let all = 1, any = 0;
          for (let o = 0; o < B; o++) { const my = y - o; if (my >= 0 && my < h) { if (rAny[my * W2 + x]) any = 1; if (!rAll[my * W2 + x]) all = 0; } else all = 0; }
          core[y * W2 + x] = all; hull[y * W2 + x] = any;
        }
        return { core: cropped(core, W2, H2), hull: cropped(hull, W2, H2) };
      };
      let legal = null;
      /** Every legal position of one variant on the sheet as it stands: L[y * PW + x] for the mask's corner at (x, y). */
      const legalMap = v => {
        const pm = v.fine.pm, PW = FW - pm.w + 1, PH = FH - pm.h + 1;
        if (PW <= 0 || PH <= 0) return null;
        const n = PW * PH;
        if (!legal || legal.n < n) legal = { n, L: new Uint8Array(n), C: new Int32Array(n), S: new Int32Array(n) };
        const L = legal.L; L.fill(0, 0, n);
        const { core, hull } = blockMasks(v), B = ratio;
        for (let by = 0; by < PH; by += B) for (let bx = 0; bx < PW; bx += B) {
          if (core && !fine.fits(core.pm, bx + core.x, by + core.y)) continue;
          const ex = Math.min(PW, bx + B), ey = Math.min(PH, by + B);
          if (hull && fine.fits(hull.pm, bx + hull.x, by + hull.y)) { for (let y = by; y < ey; y++) L.fill(1, y * PW + bx, y * PW + ex); continue; }
          for (let y = by; y < ey; y++) for (let x = bx; x < ex; x++) if (fine.fits(pm, x, y)) L[y * PW + x] = 1;
        }
        return { L, C: legal.C, S: legal.S, PW, PH, n };
      };
      /** Candidate spots for one charm at every angle: the snuggest corners of every region of legal positions, and
          the snuggest corners at each angle overall. Each spot carries its touch and the gap it sits in. */
      async function spotsFor(p) {
        const out = [], PER = 3, TOP = 10, SEP = 6;
        const keep = (list, c, k) => {
          if (list.length === k && c.touch <= list[k - 1].touch) return;
          for (let i = 0; i < list.length; i++) { const o = list[i]; if (Math.abs(o.x - c.x) <= SEP && Math.abs(o.y - c.y) <= SEP) { if (c.touch > o.touch) { list[i] = c; list.sort((a, b) => b.touch - a.touch); } return; } }
          if (list.length < k) list.push(c); else list[k - 1] = c;
          list.sort((a, b) => b.touch - a.touch);
        };
        for (const v of p.careful) {
          if (stopped()) return out;
          const map = legalMap(v); if (!map) continue;
          const { L, C, S, PW, PH, n } = map, top = [], here = [];
          C.fill(-1, 0, n);
          let regions = 0;
          for (let i0 = 0; i0 < n; i0++) {
            if (!L[i0] || C[i0] >= 0) continue;
            const id = regions++, best = []; let sp = 0; S[sp++] = i0; C[i0] = id;
            while (sp) {
              const i = S[--sp], x = i % PW, y = (i - x) / PW;
              const l = x > 0 && L[i - 1], r = x < PW - 1 && L[i + 1], u = y > 0 && L[i - PW], d = y < PH - 1 && L[i + PW];
              if (l && C[i - 1] < 0) { C[i - 1] = id; S[sp++] = i - 1; }
              if (r && C[i + 1] < 0) { C[i + 1] = id; S[sp++] = i + 1; }
              if (u && C[i - PW] < 0) { C[i - PW] = id; S[sp++] = i - PW; }
              if (d && C[i + PW] < 0) { C[i + PW] = id; S[sp++] = i + PW; }
              if ((l && r) || (u && d)) continue;   // free to move both ways along one axis: not resting on anything there
              metrics.positions++;
              const c = { v, x, y, touch: ringOccupied(v.ringFine, x - v.ringPad, y - v.ringPad) };
              keep(best, c, PER); keep(top, c, TOP);
            }
            here.push(...best);
          }
          const seen = new Set(here.map(c => c.x + ":" + c.y));
          for (const c of top) if (!seen.has(c.x + ":" + c.y)) here.push(c);
          for (const c of here) out.push(c);
          if (top[0]) probe(p, v, top[0].x, top[0].y, "turn");
          reportMetrics();
          await yieldNow();
        }
        return out;
      }
      /** Slide left against the neighbour, then up or down to the nearer charm or edge, until it stops moving. */
      const nudge = (p, v, x, y) => {
        const fits = (a, b) => fine.fits(v.fine.pm, a, b);
        for (let round = 0; round < 4; round++) {
          let moved = false;
          if (axisX) { while (fits(x - 1, y)) { x--; moved = true; probe(p, v, x, y, "nudge"); } }
          else { while (fits(x, y - 1)) { y--; moved = true; probe(p, v, x, y, "nudge"); } }
          let a = 0, b = 0;
          if (axisX) { while (fits(x, y - a - 1)) a++; while (fits(x, y + b + 1)) b++; }
          else { while (fits(x - a - 1, y)) a++; while (fits(x + b + 1, y)) b++; }
          const step = a || b ? (a <= b ? -a : b) : 0;
          if (step) { if (axisX) y += step; else x += step; moved = true; probe(p, v, x, y, "nudge"); }
          if (!moved) break;
        }
        return { x, y };
      };
      /** The best spot for one charm: every candidate graded, then the leaders nudged and graded again. */
      async function bestSpotFor(p, front) {
        const spots = await spotsFor(p);
        if (!spots.length || stopped()) return null;
        const scored = [];
        for (let i = 0; i < spots.length; i++) {
          const s = spots[i]; Object.assign(s, grade(s.v, s.x, s.y, front)); scored.push(s);
          probe(p, s.v, s.x, s.y, "try");
          if ((i & 63) === 63) { if (stopped()) return null; reportMetrics(); await yieldNow(); }
        }
        scored.sort((a, b) => b.fit - a.fit);
        if (cb.onGrades) cb.onGrades(p.id, scored.map(s => ({ angle: s.v.angle, cxPt: (s.x + s.v.solid.cx) / fineRes, cyPt: (s.y + s.v.solid.cy) / fineRes, waste: s.waste, around: s.around, growth: s.growth, fit: s.fit })), scored.length);
        let best = null;
        for (const s of scored.slice(0, 6)) {
          const n = nudge(p, s.v, s.x, s.y), g = n.x === s.x && n.y === s.y ? s : { v: s.v, x: n.x, y: n.y, ...grade(s.v, n.x, n.y, front) };
          const pick = g.fit >= s.fit - .5 ? g : s;
          if (!best || pick.fit > best.fit) best = pick;
        }
        return best;
      }
      /* A charm with no legal spot at any angle has none later either: each charm placed after it only takes room. It is
         set aside with the rest of its order, since a sheet never holds part of an order, and so is a charm that would
         take the sheet past its fill ceiling; the others go on, and the pass ends when every charm is placed or set
         aside. The usual search used to run its whole budget for such a charm, and a sheet with no room left searched
         for minutes at every later order (Paul, 24 Sep). A charm that fitted the sheet as it was, but lost its spot to a
         charm placed before it, gets a second pass that seats it first; the pass seating more charms is kept.      */
      const hadRoom = new Set();
      const fitsOn = (p, grid) => {
        const keep = fine; fine = grid;
        try { return p.careful.some(v => { const m = legalMap(v); if (!m) return false; for (let i = 0; i < m.n; i++) if (m.L[i]) return true; return false; }); }
        finally { fine = keep; }
      };
      async function pass(first) {
        for (const r of rec) probe(r.p, r.v, r.x, r.y, "lift", true);
        fine = baseFine.clone(); coarse = baseCoarse.clone(); rec = []; cells = 0;
        const left = admitted.slice(), noRoom = [], over = [], stranded = [];
        // p and the rest of its order leave; true when charms already placed were lifted, so the sheet changed
        const setAside = (p, list) => {
          const order = multi(p) ? p.order : null;
          for (const q of left.slice()) if (q === p || (order != null && q.order === order)) { left.splice(left.indexOf(q), 1); list.push(q); }
          const lifted = order == null ? [] : rec.filter(r => r.p.order === order);
          if (!lifted.length) return false;
          rec = rec.filter(r => !lifted.includes(r)); cells = rec.reduce((n, r) => n + r.v.cells, 0);
          ({ fine, coarse } = rebuildGrids(rec));
          for (const r of lifted) { list.push(r.p); probe(r.p, r.v, r.x, r.y, "lift", true); }
          return true;
        };
        if (cb.onStage) cb.onStage("careful", 0, left.length);
        while (left.length) {
          if (stopped()) return null;
          sheetValue();
          const front = reachOf();
          // best fit first: every waiting charm finds its best spot on the sheet as it is, and the best of those goes in
          const firsts = left.filter(p => first.has(p));
          let move = null;
          for (const p of firsts.length ? firsts : left.length <= 4 ? left.slice() : left.slice().sort((a, b) => b.areaPt2 - a.areaPt2).slice(0, 1)) {
            if (!left.includes(p)) continue;
            const s = await bestSpotFor(p, front);
            if (s) { hadRoom.add(p); if (!move || s.fit > move.fit) move = { p, ...s }; continue; }
            if (stopped()) return null;
            if (rec.length && (hadRoom.has(p) || fitsOn(p, baseFine))) stranded.push(p);
            if (setAside(p, noRoom)) { move = null; break; }
            if (move && !left.includes(move.p)) move = null;
          }
          if (!move) continue;
          const { p, v, x, y } = move;
          if (!fine.fits(v.fine.pm, x, y)) return null;
          if ((cells + v.cells) / usableCellsFine > maxFill + 1e-9) { setAside(p, over); continue; }
          fine.stamp(v.fine.bits, v.fine.w, v.fine.h, x, y, p.id);
          for (let yy = 0; yy < v.fine.h; yy++) for (let xx = 0; xx < v.fine.w; xx++) if (v.fine.bits[yy * v.fine.w + xx]) { const gx = Math.floor((x + xx) / ratio), gy = Math.floor((y + yy) / ratio); if (gx >= 0 && gy >= 0 && gx < coarse.W && gy < coarse.H) coarse.set(gx, gy); }
          coarse.buildSAT();
          rec.push({ p, v, x, y, fit: move.fit, waste: move.waste }); cells += v.cells;
          left.splice(left.indexOf(p), 1);
          probe(p, v, x, y, "place", true);
          const pl = { id: p.id, angle: v.angle, cxPt: (x + v.solid.cx) / fineRes, cyPt: (y + v.solid.cy) / fineRes, xPt: (x + (v.fine.w - v.solid.w) / 2) / fineRes, yPt: (y + (v.fine.h - v.solid.h) / 2) / fineRes, wPt: v.solid.w / fineRes, hPt: v.solid.h / fineRes, careful: { waste: +move.waste.toFixed(2), around: +move.around.toFixed(2), growth: +move.growth.toFixed(2) } };
          if (cb.onPlaced) cb.onPlaced(pl, { trial: 0, placed: rec.length, total: prepared.length, careful: true });
          if (cb.onStage) cb.onStage("careful", rec.length, admitted.length);
        }
        return { rec, fine, coarse, cells, noRoom, over, stranded, fit: rec.reduce((n, r) => n + r.fit, 0) };
      }
      let out = await pass(new Set()), passes = 1;
      if (out && out.stranded.length) {
        passes = 2;
        const again = await pass(new Set(out.stranded));
        if (!again) return null;
        if (again.rec.length > out.rec.length || (again.rec.length === out.rec.length && again.fit > out.fit)) out = again;
        else { for (const r of again.rec) probe(r.p, r.v, r.x, r.y, "lift", true); for (const r of out.rec) probe(r.p, r.v, r.x, r.y, "place", true); }
      }
      if (!out) return null;
      ({ rec, fine, coarse, cells } = out);
      const placements = rec.map(r => ({ id: r.p.id, angle: r.v.angle, cxPt: (r.x + r.v.solid.cx) / fineRes, cyPt: (r.y + r.v.solid.cy) / fineRes, xPt: (r.x + (r.v.fine.w - r.v.solid.w) / 2) / fineRes, yPt: (r.y + (r.v.fine.h - r.v.solid.h) / 2) / fineRes, wPt: r.v.solid.w / fineRes, hPt: r.v.solid.h / fineRes }));
      const ids = new Set(placements.map(p => p.id)), capped = (fifo ? prepared.filter(p => rank.get(p.order) >= capOrders).map(p => p.id) : []).concat(out.over.map(p => p.id));
      metrics.layouts++; reportMetrics(true);
      return { placements, rejects: prepared.filter(p => !ids.has(p.id)).map(p => p.id), capped, noRoom: out.noRoom.map(p => p.id), density: cells / usableCellsFine, contactQuality: qualityOf(rec, fine), trial: 0, stripPacked: false,
        usablePt2: usableCellsFine / (fineRes * fineRes), freePt2: fine.freeCells() / (fineRes * fineRes), placedPt2: cells / (fineRes * fineRes), placedCells: cells, pocket: pocketPt(coarse, coarseRes),
        grids: { fine, coarse }, rec, fitScore: out.fit, wastePt2: rec.reduce((n, r) => n + r.waste, 0) * MM_PX * MM_PX / (fineRes * fineRes), careful: { graded, angles: angles.length, turn, passes } };
    }
    let carefulDone = false;
    if (carefulOn && !best) {
      const careful = await carefulAppend();
      // every charm is placed, held back by the fill ceiling, or shown to have no spot at any angle: nothing is left to search
      if (careful && careful.rejects.every(id => careful.capped.includes(id) || careful.noRoom.includes(id))) {
        best = careful; carefulDone = true; endedBy = !careful.rejects.length ? "complete" : careful.noRoom.length ? "no-room" : "cap";
        if (cb.onBest) cb.onBest(publicLayout(best), { trial: 0, placed: best.placements.length, total: prepared.length, rejects: best.rejects.slice(), density: best.density, elapsedMs: now() - t0, careful: true });
      }
    }
    const refinementReserve = Math.min(10000,budget*.2);
    while (!carefulDone && trials < maxTrials) {
      if (job.packingHints && job.packingHints !== lastAdvice) { lastAdvice = job.packingHints; lastBetterAt = now(); }
      if (stopped()) { endedBy = cb.shouldStop && cb.shouldStop() ? "stopped" : stalled ? "stalled" : "budget"; break; }
      if (best?.placements.length && now()-t0 >= budget-refinementReserve) break;
      const trial = trials++;
      stripWeight = job.exploreRotations ? [4, .15, .04, .5][trial % 4] : 4;
      // ordering: trial 0 = pure largest-first; later trials add noise growing with the streak
      const sigma = trial === 0 ? 0 : Math.min(0.6, 0.15 + 0.05 * Math.min(failStreak, 8));
      // pieces the previous trial could not place go first (most of the time), so the layout is built around them
      const front = trial > 0 && lastRejects.length && random() < 0.75 ? new Set(lastRejects) : new Set();
      const targetOrders = capOrders;
      const candidates = fifo ? byAreaDesc.filter(p => rank.get(p.order) < targetOrders) : byAreaDesc;
      // Sparse queues consume a strip from one edge. Alternate construction
      // seeds escape contact-only local optima; final ranking and refinement
      // always measure contact against the actual silhouettes, never the walls.
      const setCells = candidates.reduce((n, p) => n + p.footprintCells, 0), sparse = setCells / usableCellsFine < maxFill * 0.9;
      // A queue that fills most of the sheet leaves no offcut to keep, and a left-to-right build can fail to seat it
      // where a build from the contacts seats every charm (real charms, 23 Sep: 3 of 4 sets whose last order missed
      // fitted only this way). Such a queue alternates the two builds; of two complete layouts the ranking keeps the
      // more compact one. Rose Gold keeps its left-to-right build for the stock its green line leaves.
      const contactBuild = !!job.nearFullContact && sparse && !roseAxis && setCells / usableCellsFine >= maxFill * 0.75 && trial % 2 === 1;
      fitNow = fitLine && trial % 2 === 0;
      const stripPacked = !contactBuild && (fitNow || sparse || ((job.exploreRotations ? trial % 3 === 2 : trial > 0) && best?.density < maxFill * 0.9));
      const advice = job.packingHints || {}, priorities = new Map((advice.priority || []).map((id,i,a) => [id, 1 - i / Math.max(1,a.length)]));
      const shaped = !!advice.profiles && candidates.every(p => advice.profiles[p.id]);
      const guided = shaped || (trial % 3 === 1 && priorities.size);
      for (const p of prepared) p.guidance = shaped ? advice : null;
      if (guided) guidedTrials++;
      // Offset the angle lattice between restarts. Imported artwork already has
      // arbitrary orientations: repeating 0/30/60 forever can exclude good fits.
      const angleOffset = job.exploreRotations && angles.length < 180 ? (trial % Math.max(2, Math.round(360/angles.length/5))) * 5 : 0;
      for (const p of candidates) if (!p.pinned) p.variants = variantsFor(p, angles.map(a=>(a+angleOffset)%360));
      if (guided) for (const p of candidates) if (!p.pinned && advice.angles?.[p.id]) p.variants = variantsFor(p, [...new Set(angles.concat(advice.angles[p.id].filter(Number.isFinite).slice(0,3).map(a => ((a % 360) + 360) % 360)))]);
      // Some constructions seat complete oldest orders first. A largest-first
      // full-sheet trial can otherwise fill with younger pieces, fail one old
      // order, then throw most of the occupied sheet away to enforce FIFO.
      const chronological = fifo && trial % 3 === 1;
      const order = candidates.map(p => ({ p, k: (front.has(p.id) ? 1e9 : 0) + p.areaPt2 * (1 + sigma * (random() * 2 - 1) + (guided ? .8 * (priorities.get(p.id) || 0) : 0)) })).sort((a, b) => (chronological ? rank.get(a.p.order) - rank.get(b.p.order) : 0) || b.k - a.k).map(o => o.p);
      const pinnedFirst = order.filter(p => p.pinned).concat(order.filter(p => !p.pinned));
      const gravW = trial === 0 ? 0.35 : 0.1 + random() * 0.8;
      const noise = trial === 0 ? 0 : random() * 0.15;
      const cornerX = trial === 0 ? 0 : (random() < 0.7 ? 0 : 1), cornerY = trial === 0 ? 0 : (random() < 0.7 ? 0 : 1);
      let fine = baseFine.clone(), coarse = baseCoarse.clone();
      let placements = [], placedRec = []; const capped = fifo ? prepared.filter(p => rank.get(p.order) >= capOrders).map(p => p.id) : [], rejects = capped.slice();
      let placedCells = 0, groupMoves = [], trialInterrupted = false;
      const useGroups = !!gpu && !!cb.groupSearch && trial % 2 === 1;
      const deadOrders = new Set();
      // a piece of a multi-piece order failed: the order leaves this sheet whole — its placed siblings are lifted back off
      const dropOrder = (p, why) => {
        if (!multi(p)) return;
        groupMoves = [];
        deadOrders.add(p.order);
        const lifted = placedRec.filter(r => r.p.order === p.order);
        if (!lifted.length) return;
        placedRec = placedRec.filter(r => r.p.order !== p.order);
        placements = placements.filter(pl => !lifted.some(r => r.p.id === pl.id));
        for (const r of lifted) { placedCells -= r.v.cells; rejects.push(r.p.id); if (why === "cap") capped.push(r.p.id); if (cb.onReject) cb.onReject(r.p.id, trial, "order"); }
        ({ fine, coarse } = rebuildGrids(placedRec));
      };

      for (let pi = 0; pi < pinnedFirst.length; pi++) {
        if (shaped && !pinnedFirst[pi].pinned) {
          // Guidance chooses the next charm on EVERY construction, not just one
          // occasional restart. Randomness only breaks equal guidance scores.
          const tail=pinnedFirst.slice(pi).sort((a,b)=>(chronological ? rank.get(a.order)-rank.get(b.order) : 0) || guidedOrderScore(b,placedRec,advice)-guidedOrderScore(a,placedRec,advice));
          pinnedFirst.splice(pi,tail.length,...tail);
        }
        if (stopped() || (trial > 0 && best?.placements.length && now()-t0 >= budget-refinementReserve)) { trialInterrupted = true; break; }
        if(useGroups && gpu && !pinnedFirst[pi].pinned && !groupMoves.length){
          const remaining=pinnedFirst.slice(pi).filter(p=>!p.pinned&&!deadOrders.has(p.order));
          try{groupMoves=await cb.groupSearch({gpu,pieces:remaining,remaining,fine,coarse,ratio,placedCells,maxCells:usableCellsFine*maxFill,strip:stripPacked?stripOf(placedRec):null,deadline:Math.min(t0+budget-refinementReserve,now()+1200),shouldStop:stopped,metrics,report:reportMetrics,seed:job.seed});}
          catch(e){gpu?.destroy?.();gpu=null;groupMoves=[];cb.onGPU?.({active:false,reason:'CPU fallback: '+String(e.message||e)});}
        }
        if(groupMoves.length){const target=pinnedFirst.findIndex((p,i)=>i>=pi&&p.id===groupMoves[0].p.id);if(target<0)groupMoves=[];else [pinnedFirst[pi],pinnedFirst[target]]=[pinnedFirst[target],pinnedFirst[pi]];}
        const p = pinnedFirst[pi];
        if(stopped()){trialInterrupted=true;break;}
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
          metrics.positions++;reportMetrics();
        } else {
          if(groupMoves[0]?.p.id===p.id){const proposed=groupMoves.shift();metrics.positions++;if(fine.fits(proposed.v.fine.pm,proposed.x,proposed.y))bestPos=proposed;else groupMoves=[];}
          if(!bestPos)bestPos = await measuredSearch(p, fine, coarse, ratio, gravW, noise, random, stripPacked ? 0 : cornerX, stripPacked ? 0 : cornerY, stripPacked ? stripOf(placedRec) : null, sparse ? trial % 3 === 2 : (job.exploreRotations ? trial % 3 === 0 : trial === 0));
          if(!bestPos&&stopped()){trialInterrupted=true;break;}
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
          fine.stamp(v.fine.bits, v.fine.w, v.fine.h, x, y, p.id);
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
          // Publish completed legal prefixes during construction, not only at
          // restart boundaries. Speculative/partial orders never reach the UI.
          if (placements.length > (best?.placements.length || 0)) {
            const ids = new Set(placements.map(p => p.id));
            const incomplete = prepared.filter(p => !ids.has(p.id));
            const cutoff = fifo && incomplete.length ? Math.min(...incomplete.map(p => rank.get(p.order))) : Infinity;
            const bad = new Set(incomplete.map(p => p.order));
            const valid = placedRec.filter(r => !bad.has(r.p.order) && rank.get(r.p.order) < cutoff);
            if (valid.length > (best?.placements.length || 0)) {
              const grids = rebuildGrids(valid), cells = valid.reduce((n,r) => n+r.v.cells,0);
              const validIds = new Set(valid.map(r => r.p.id));
              if (cells / usableCellsFine <= maxFill) {
                best = { placements: placements.filter(p => validIds.has(p.id)), rejects: prepared.filter(p => !validIds.has(p.id)).map(p => p.id), capped: capped.slice(), density: cells/usableCellsFine, contactQuality: qualityOf(valid,grids.fine), trial, stripPacked, usablePt2: usableCellsFine/(fineRes*fineRes), freePt2: grids.fine.freeCells()/(fineRes*fineRes), placedPt2: cells/(fineRes*fineRes), placedCells: cells, pocket: pocketPt(grids.coarse,coarseRes), grids, rec: valid.slice() };
                lastBetterAt = now(); failStreak = 0;
                cb.onBest?.(publicLayout(best), { trial, placed: best.placements.length, total: prepared.length, rejects: best.rejects.slice(), density: best.density, elapsedMs: now()-t0, incremental: true });
              }
            }
          }
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
        if (chronological && rejects.some(id => rank.get(prepared.find(p => p.id === id).order) < capOrders)) break;
      }

      // A budget/stop during a trial must not make the unvisited pieces disappear.
      for (const p of prepared) if (!placements.some(pl => pl.id === p.id) && !rejects.includes(p.id)) rejects.push(p.id);
      // A deadline can interrupt an order before its remaining pieces are even
      // tried. Remove incomplete orders BEFORE comparing/publishing this trial;
      // otherwise final cleanup can turn the selected "best" into a worse one.
      for (const id of rejects.slice()) {
        const p = prepared.find(p => p.id === id);
        if (p) dropOrder(p, "unfinished");
      }
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
      if(!trialInterrupted)metrics.layouts++;reportMetrics(true);
      const summary = {
        completedTrials:metrics.layouts, completed:!trialInterrupted, groupTrial:useGroups, trial, placed: placements.length, total: prepared.length, rejects,
        density: placedCells / usableCellsFine,
        elapsedMs: now() - t0, gravW, noise
      };
      const contactQuality = qualityOf(placedRec, fine);
      const better = betterLayout({ placements, density: summary.density, contactQuality, envelopePt2: envelopePt2(placedRec) }, withEnvelope(best), job.sheet);
      if (better) {
        failStreak = 0; lastBetterAt = now();
        best = { placements, rejects, capped: capped.slice(), density: summary.density, contactQuality, trial, stripPacked, usablePt2: usableCellsFine / (fineRes * fineRes),
          freePt2: fine.freeCells() / (fineRes * fineRes), placedPt2: placedCells / (fineRes * fineRes), placedCells,
          pocket: pocketPt(coarse, coarseRes), grids: { fine, coarse }, rec: placedRec.slice() };
        if (cb.onBest) cb.onBest(publicLayout(best), summary);
        // one or two short (and not because of the cap): targeted push into this very layout
        if (!fifo && repairCandidates(best).length <= 2) await finishPush(best);
      } else failStreak++;
      if (best && await extendOldest(best)) failStreak = 0;
      if (cb.onTrial) cb.onTrial(Object.assign({ better }, summary));
      if (best && best.placements.length === prepared.length) {
        endedBy = "complete";
        // Seating every piece is not enough on a partial sheet: spend a few
        // bounded restarts shortening the occupied strip before publishing it.
        // A requested full optimization keeps searching through its budget.
        if (!job.fullBudget && trials >= 8 && now()-lastBetterAt >= Math.min(stallMs || 15000,15000)) break;
      }
      // restarts stalling while the best is a few short → repair the best layout instead
      if (best && now()-t0 < budget-refinementReserve && repairCandidates(best).length > 0 && repairCandidates(best).length <= 3 && failStreak >= 4) {
        failStreak = 0;
        if (cb.onStage) cb.onStage("repair", 0, 6);
        const fixed = await ruinRecreate(best, 6);
        if (fixed) { lastBetterAt = now(); if (best.placements.length === prepared.length) endedBy = "complete"; }
      }
      if (best && best.rejects.length && best.rejects.every(id => best.capped.includes(id))) { endedBy = "cap"; if (!job.fullBudget && trials >= 8 && now()-lastBetterAt >= Math.min(stallMs || 15000,15000)) break; }   // only the ceiling holds pieces back
      await yieldNow();
    }
    if (trials >= maxTrials && endedBy === "budget") endedBy = "trials";
    // Make room in the emptiest corners without exposing temporary removals.
    // Only a fully rebuilt layout containing more complete orders can win.
    if(best?.rejects?.length && best.rec?.length && !stopped() && !carefulDone) {
      await ruinRecreate(best,8,{deadline:Math.min(t0+budget,now()+refinementReserve*.5)});
    }
    // Construction heuristics propose layouts; neighbour and edge contact judge
    // them. Reinsert unpinned pieces across rotations, retaining the best layout
    // by the same count/offcut/contact ordering used by the worker pool. A careful
    // append is already graded and nudged: this polish, which pulls pieces to the
    // left end of the strip, would undo it.
    if (best?.rec?.length && !carefulDone && !(cb.shouldStop && cb.shouldStop())) {
      stripWeight = 4; fitNow = fitLine;
      best.contactQuality = qualityOf(best.rec, best.grids.fine);
      best.contactQualityBefore = best.contactQuality;
      const movable = best.rec.filter(r=>!r.p.pinned);
      const extent = r => stripAxis === "x" ? r.x+r.v.fine.w : r.y+r.v.fine.h;
      const perimeterBand = Math.max(1,Math.round(2*72/25.4*fineRes)); // 2 mm inside the usable cut boundary
      const loose = [...new Set([
        ...movable.filter(r=>placementAt(r.v,best.grids.fine,r.x,r.y).edge>0).sort((a,b)=>straightEdgeAt(a.v.fine,best.grids.fine,a.x,a.y,a.v.ringPad)-straightEdgeAt(b.v.fine,best.grids.fine,b.x,b.y,b.v.ringPad)).slice(0,8),
        ...movable.slice().sort((a,b)=>extent(b)-extent(a)).slice(0,8),
        ...movable.slice().sort((a,b)=>b.p.areaPt2-a.p.areaPt2).slice(0,6),
        ...movable.slice().sort((a,b)=>placementAt(a.v,best.grids.fine,a.x,a.y).score-placementAt(b.v,best.grids.fine,b.x,b.y).score).slice(0,12)
      ])];
      for (const original of loose) {
        if (now() - t0 > budget || (cb.shouldStop && cb.shouldStop())) break;
        const r = best.rec.find(x => x.p.id === original.p.id), keep = best.rec.filter(x => x !== r);
        const grids = rebuildGrids(keep);
        const policy=best.density >= maxFill*.9 ? {band:perimeterBand,minCells:edgeBandCells(r.v.fine,grids.fine,r.x,r.y,perimeterBand)} : null;
        const pos = await measuredSearch({...r.p,variants:variantsFor(r.p,[...new Set(angles.concat(angles.length < 180 ? angles.map(a=>(a+15)%360) : [],r.v.angle))])}, grids.fine, grids.coarse, ratio, .1, 0, random, 0, 0, best.density < maxFill * .9 ? stripOf(keep) : null,false,policy);
        if (!pos || (pos.x === r.x && pos.y === r.y && pos.v.angle === r.v.angle)) continue;
        // On a full sheet, a one-piece polish must not trade occupied edge
        // space for interior neighbour contact. Sparse sheets still compact
        // freely into a strip to preserve their reusable offcut.
        if (best.density >= maxFill*.9 && edgeBandCells(pos.v.fine,grids.fine,pos.x,pos.y,perimeterBand) < edgeBandCells(r.v.fine,grids.fine,r.x,r.y,perimeterBand)) continue;
        const moved = {...r,v:pos.v,x:pos.x,y:pos.y}, rec = best.rec.map(x => x === r ? moved : x);
        const placements = best.placements.map(p => p.id !== r.p.id ? p : {...p,angle:pos.v.angle,cxPt:(pos.x+pos.v.solid.cx)/fineRes,cyPt:(pos.y+pos.v.solid.cy)/fineRes,xPt:(pos.x+(pos.v.fine.w-pos.v.solid.w)/2)/fineRes,yPt:(pos.y+(pos.v.fine.h-pos.v.solid.h)/2)/fineRes,wPt:pos.v.solid.w/fineRes,hPt:pos.v.solid.h/fineRes});
        const nextGrids = rebuildGrids(rec), contactQuality = qualityOf(rec,nextGrids.fine);
        const placedCells = best.placedCells-r.v.cells+pos.v.cells;
        const candidate = {...best,placements,rec,grids:nextGrids,contactQuality,placedCells,placedPt2:placedCells/(fineRes*fineRes),density:placedCells/usableCellsFine};
        if (candidate.density <= maxFill && betterLayout(withEnvelope(candidate),withEnvelope(best),job.sheet)) {
          if (pos.v.angle !== r.v.angle) candidate.rotationRefinements = (best.rotationRefinements || 0)+1;
          best = candidate; best.contactRefinements = (best.contactRefinements || 0) + 1;
          best.extended = false; // Moving an edge piece may have opened a slot for the next whole order.
          best.pocket = pocketPt(nextGrids.coarse,coarseRes);
          best.freePt2 = nextGrids.fine.freeCells()/(fineRes*fineRes);
          if (cb.onBest) cb.onBest(publicLayout(best),{trial:best.trial,placed:placements.length,total:prepared.length,rejects:best.rejects,density:best.density,elapsedMs:now()-t0,refining:true});
          await extendOldest(best);
        }
        await yieldNow();
      }
    }
    if (best && best.rec && fitLine) best.envelopePt2 = envelopePt2(best.rec);
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
    if (best.placements.length === pieces.length && endedBy !== "stopped") endedBy = "complete";
    // what the pieces the ceiling held back would add, so the console can say the arithmetic plainly
    const cappedPt2 = (best.capped || []).reduce((n, id) => { const p = prepared.find(x => x.id === id); return n + (p ? p.footprintCells / (fineRes * fineRes) : 0); }, 0);
    reportMetrics(true);
    return Object.assign({}, best, {
      endedBy, trials:metrics.layouts, metrics:{...metrics}, elapsedMs: now() - t0, cappedPt2,
      params: { exploreRotations: !!job.exploreRotations, contactScoring: "silhouette-neighbors-and-edges", edgeWeight: EDGE_WEIGHT, straightEdgeWeight: STRAIGHT_EDGE_WEIGHT, perimeterCandidates: true, compactPartial: !!best.stripPacked, packingAxis: best.stripPacked ? stripAxis : null, packingGuided: guidedTrials > 0, shapeGuided: !!job.packingHints?.profiles, guidedTrials, seed: job.seed == null ? 1 : job.seed, angles, clearancePt, insetPt, fineRes, coarseRes, maxFill, pieceOrder: byAreaDesc.map(p => p.id) }
    });
  }

  /** Farthest occupied cell of a mask along each line of the Rose Gold cut
   *  axis: rows for axis "x", columns for axis "y". Cached per mask. */
  function lineProfile(mask, axis) {
    const key = axis === "x" ? "rowsFar" : "colsFar"; if (mask[key]) return mask[key];
    const bits = mask.bits || unpack(mask.pm), w = mask.w, h = mask.h, far = new Int32Array(axis === "x" ? h : w).fill(-1);
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) if (bits[y * w + x]) {
      const line = axis === "x" ? y : x, d = axis === "x" ? x : y;
      if (far[line] < d) far[line] = d;
    }
    return mask[key] = far;
  }
  // Per coarse cell of mean line advance, against 1 for each cell the strip front grows.
  const LINE_FIT_WEIGHT = .5;
  /** Cells a mask at (x, y) adds beyond the envelope E, the depth already consumed on each line. */
  function envelopeGrowth(E, far, x, y, axis) {
    let g = 0; const base = axis === "x" ? y : x, off = axis === "x" ? x : y;
    for (let j = 0; j < far.length; j++) { if (far[j] < 0) continue; const d = off + far[j] + 1 - E[base + j]; if (d > 0) g += d; }
    return g;
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

  /* ── slivers: free space no charm can use ────────────────────────────────
     A free cell is usable when a disc SLIVER_MM wide fits over it without touching a charm or the inset band; a
     charm narrower than that has no body to put there (the thickest part of the shop's charms is 5.4 mm across
     at the median, 3.3 mm for the thinnest 5%). The rest are slivers: gaps between charms and along the edges
     that stay empty for good. Distances are 3-4 chamfer steps, in thirds of a cell.                      */
  const SLIVER_MM = 5;
  // a careful append's grade, in mm² given up: slivers count in full, the empty band around the charm at `around`,
  // each mm the occupied part of the sheet grows at `growth`, and each mm further along it at `along`
  const FIT_WEIGHTS = { around: .5, growth: 1.5, along: .02 };
  // how far out from a charm's outline the empty space around it counts
  const AROUND_MM = 3;

  /* ── what free space is worth ──────────────────────────────────────────────
     A gap is worth what later charms can do with it. The free space a disc SLIVER_MM wide can sweep splits into
     regions: pockets, voids, the open end of the sheet. Each cell of a region is worth the share of PROBES that fit
     in it; slivers are worth nothing and the open sheet 1. PROBES are 24 of the shop's library charms, the middle
     one of each 24th of the library ordered by area (27 to 117 mm², 5 to 11 mm at their narrowest), each drawn at
     1 px/pt, one bit per px, rows first, in base64, and tried at 45° steps.                                    */
  const PROBES = [
    [29, 20, "AAAAAAAAfgAA/D+A4f8H+P7/AP//D8D//4H//w/+/3/A//8D+P8/AP//AcD/DwDw/wMAAPwAAIAfAADwAwAAfgAAgAcAAAAAAA=="],
    [19, 31, "AAAAPwD4A8B/AP4D/D/w/4H/D/5/8H+A/wP8D+B/AP8D8B8A/ADgBwA/APgDwJ8D/j/w/4H/D/wf4H8A/wP4H8D/AP4H4D8AAAA="],
    [29, 29, "AAAAADwAAIAHAADwAQAAfgAA/A8AgP8DAPD/AAD+n9/P////8f//H/7/P4D//wPw/z8A/P8DgP8/AMD/BwDg/wAA+A8AgP8BAPAfAAD/AADgDwAA/AEAwD8AAPgDAAA/AAAAAAAAAAAAAA=="],
    [21, 28, "AAAA8AMA/wDwPwD+B8D/APgfAP8D4H8A/AfAfwD8D8D/H/j/h///8f9//v/P///5/z///wf+/8D/H/j/Af4/wP8D8HgADAIAAAA="],
    [29, 24, "AAAAgA8AAPgBAAB/AADgDwAA/AEAgB8AAPDBHwD+/h+A//9/4P//H/j//wf+//8A//8fwP//A/z/P8D//wH8/z/A//8H+P3/A5//f8Djvw84/GcAAAAA"],
    [26, 29, "AAAAAPwAAPwHAPgfAOD/AOD/A+D/D4D/PwD+fwDg/wEA/D8O+P9/8P//wf//B///H/z/P/D//8D//wP//wf4/w/A/x8A/B8A/B8A8P8AwP8DAP4HAAAfAAA4AAAAAAA="],
    [31, 34, "AAAAAADgBwAA+AcAAPwDAAD+AQAA/wAAgH8AAOAfAADwDwAA/AcAAP8HAMD/A8D//z/w//9/+P//f/z//3/+//8//P//D/D//wHw/z8A8P8PAPD/AwD4/wAA+B8AAPgHAADwAQAAeAAAAHgAAAD8AAAA/AEAAPwBAAD4AAAAcAAAAAAA"],
    [25, 39, "AAAA8AMA8B8A4D8AgP8AAP8DAP8PAP8/AP5/APz/Afj/B/D/D8D3HwDAfwKA/w8A/z/A/n+A//+B//8D/v8H/P8PwP8PgP8fAP8ZAP4DAP4HAPwHAPwPAPgPAPgfAPAfAOAfAOAfAOA/AOA/AMB/AAA/AAAaAAAAAAA="],
    [24, 25, "AAAA8MEP+MMf/Oc//v9//v9//v9//v9//v9//P8/+P8f8P8P4P8H8P8P+P8f/P8//v8//v9//v9//v9//v9//Oc/+MMf8MEPAAAA"],
    [29, 29, "AAAAAID/BwD4/wAA/z8A/P9/wP//D/j//4H//z/4//8H////wP//P/j//wP+/3/A//8P/v//4P//A/z/P8D//w/4//8B/38e4P+HA/z/AIDnHwAA+AEAAD4AAIAHAABgAAAAAAAAAAAAAA=="],
    [26, 26, "AAAAAP4PAPz/APz/B/j/P+D//8H//wf//z/+///4///j//+f//9//v//+f//5///n///P/7///j//8P//wf+/x/4/z/A/38A/v8A4P8AAHwAAAAAAA=="],
    [36, 30, "AAAAAAAA+AEAAIA/AAAA/AMAAMA/AAAA/AMAAID/AQAA+B9gAID/wAcA/Ad+AP9/8AP+/wM/+P8/+MP//78//v///+P///8f/v///+H///8f/v///4H///8/8P//4wP+/x8+gP8/AAPg/wEAAPwfAADA/wEAAHg/AACAxwMAAGAAAAAAAAAA"],
    [31, 23, "AAAAAP4O/4f/j//H/+f/5//3//P////5/////P//f/7//x////8P////h////4H//3+A//8/4P//H/D//w/4//8H+P//A/z//wD8/38A/PMfAHzgAwAAAAAA"],
    [27, 28, "AAAA8AMAjx9//Pz+7+f3fz/+///x///H//8/vv//8f1/n+//+3z+z+f/fz/////x//+H//8/8P//4P//D///f/z//+f/5z////75//fP/78//P75wPMHAAAeAAAAAAA="],
    [21, 48, "AAAA4AMA/gDAHwD8AwB/gP//8f9//v/P///5/z/+/wf8B4D/APAfwP8P+P8D/3/g/w/8/4H/P/D/B/7/wP8f+P8D/3/g/w/8/4H/P/D/B/7/wP8f+P8D/3/g/wf4/wD/D8D/AMAHAHgAAA8A4AEAPACABwDwAAAeAMABAAAA"],
    [37, 36, "AAAAAAAAAABwAAAAwB8AAAD8AwAAwP8AABz4HwDgh/8DAPz9PwDA//8HAPj//wAA//8PAMD//wAA8P8PAAD8/wEAgP8fAAD4/wEAgP8/AOD//wMA/v8/AMD//wMA+P8/AAD4/wcAgP8/AAD8//8BwP//PwD8//8HwP///wD8//8f4P9//wP8/8M/gP8/8APw/wAAAP4HAAAAPwAAAIAAAAAAAAAAAAA="],
    [34, 31, "AAAAAAA/AAAA/gAAAPgHAADgHwAAgH+AAQD+AT8A8Pf/AcDv7wcA//8fAPj/PwDg//8AgP//AwD8/wcA8P8fAID/fwAA/v8AAPj/A0Dg/w+Az/9/fv7////5////4////w////8f/P//f/D////h////n////3/+/X/+wMP/+AAAAAAA"],
    [29, 36, "AAAAAAA+AADgDwAA/AEAgH8AAPAHAAD/AADwPwAA/g8AwP8BAPw/AID/B8Dz/zz4/t8Pv//7wff/H/D+/wD+/x/4//87////5/7///z//5/////z//9//P//B///f8D//wf+///B//9/+P/3Dw//4OH9/T28v78H9/d3AD74AAAAAAA="],
    [29, 37, "AAAAAAA/AADwBwAA/gEAwD8AAPgHAAD/HwD+/x/g//8H////8P//H/7//+P//3/8//8P////4f//H/j/fwP+/x/A//8DwP8/APj/BwD+fwDA/wcA/P8BwP8/APj/B4D//wDw/x8A//8D7P9/gP//D/D//wH8/38A//8PgP//AQB+DAAAAAAA"],
    [24, 44, "AAAAfAAA/gAA/gEA/gEA/gEA/gAA/AAA/AEA/AEA/AEe/AE//AE//AF//AF//IE/+P8/+P8/+P8/+P8/+P8/+P8//P8//P8//v8//v8//v8//v8//v8//v8//v8//P8f/P8f/P8f+P8f8P8P4P8P4P8PwP8HwP8HwP8HgP8HgP8HAAAA"],
    [30, 35, "AAAAAAD8AACAPwAA4B8AAPgHAAD+AQCAPwAA8D8AAP4fAID/BwDw/wMA//8P+P//B////+P////4//9//v//n////+f////5//9//v//n////8P//3/g//8P8P//B/z//wH//3/A//8f8P//B/z//wD+/z+A//8HwP//AOAPDwAAAAAA"],
    [32, 35, "AAAAAAAA/AAAAPwBAAD+AQAA/gEAAP4BAAD8AQAA/gEAD/8DwD//A+B//wfgf/8H4H//B/D//wfw//8P8P//D/D//w/4//8P+P//D/j//x/4//8f+P//H/z//x/8//8//P//P/z//z/+//8//v//P/7//z/+//8//v+/P/7/Bxz+/wEAPPgAAAAAAAA="],
    [31, 36, "AAAAAADwAQAA/AEAAP8AAIB/AADAPwAA8H8AAP7/AMD//wDw//8B/P//Af///4D////g////8P//f/z//3/+//8/////n////8/////n////8/////n////8//9//v//P////x////+H////g////8D//z/A//8fwP//B8D//wCA/z8AAP8HAAAAAAA="],
    [35, 42, "AAAAAAAAfgAAAPAHAADAPwAAAP4BAADwDwAAgP8AAID/HwAA//8DAPz/PwDw//8HwP//fwD///8D/P//P+D///+D////H/z////x////j////3/8////5////z//////+f///8////9//v////P///+f//////z////n////H//////w////h////x/4////wP///wP8//8fwP//fwD+//8BwP//BwD8/x8AwP8/AADwfwAAAAAAAA=="],
  ];
  // a region with a free circle this wide (mm) takes nearly every library charm, so it is not tried with PROBES
  const OPEN_MM = 14;
  // how far from a new charm (mm) a pocket it closes off may reach and still be seen
  const POCKET_MM = 9;
  function bitsFromBase64(s, n) {
    const T = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", out = new Uint8Array(n);
    let acc = 0, bits = 0, k = 0;
    for (let i = 0; i < s.length && k < n; i++) {
      const c = T.indexOf(s[i]); if (c < 0) continue;
      acc = ((acc & 0x3ff) << 6) | c; bits += 6;
      if (bits >= 8) { bits -= 8; const byte = (acc >> bits) & 255; for (let b = 0; b < 8 && k < n; b++) out[k++] = (byte >> b) & 1; }
    }
    return out;
  }
  const probeCache = new Map();
  /** The PROBES as packed masks on a grid of `fineRes` px/pt, grown or shrunk like the pieces, at 45° steps. */
  function probeVariants(fineRes, erodeFine, halfGapFine) {
    const key = fineRes + ":" + erodeFine + ":" + halfGapFine;
    if (!probeCache.has(key)) probeCache.set(key, PROBES.map(([w, h, b]) => {
      const f = resample(bitsFromBase64(b, w * h), w, h, 1, fineRes), out = [];
      for (let a = 0; a < 360; a += 45) {
        const rot = rotateBitmap(f.bits, f.w, f.h, a); if (!rot.w) continue;
        const m = erodeFine ? erode(rot.bits, rot.w, rot.h, erodeFine) : dilate(rot.bits, rot.w, rot.h, halfGapFine);
        if (m.w && areaOf(m.bits)) out.push({ pm: packShifted(m.bits, m.w, m.h), w: m.w, h: m.h, cells: areaOf(m.bits) });
      }
      return out;
    }));
    return probeCache.get(key);
  }
  /** Share of the probes that fit in region `id` of a domain {W, H, occ, label}: the region's own cells, and free
      cells of no region (slivers) within `pad` steps of it, are open; everything else is solid. */
  function regionShare(D, id, bb, probes, pad) {
    const x0 = Math.max(0, bb[0] - pad), y0 = Math.max(0, bb[1] - pad), x1 = Math.min(D.W, bb[2] + pad), y1 = Math.min(D.H, bb[3] + pad);
    const w = x1 - x0, h = y1 - y0, n = w * h, dist = new Int16Array(n).fill(-1), q = new Int32Array(n);
    let head = 0, tail = 0, open = 0;
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) if (D.label[(y + y0) * D.W + x + x0] === id) { dist[y * w + x] = 0; q[tail++] = y * w + x; }
    while (head < tail) {
      const i = q[head++], d = dist[i]; open++;
      if (d >= pad) continue;
      const x = i % w, y = (i - x) / w;
      for (let k = 0; k < 4; k++) {
        const j = k === 0 ? (x > 0 ? i - 1 : -1) : k === 1 ? (x < w - 1 ? i + 1 : -1) : k === 2 ? (y > 0 ? i - w : -1) : (y < h - 1 ? i + w : -1);
        if (j < 0 || dist[j] >= 0) continue;
        const g = (((j / w) | 0) + y0) * D.W + (j % w) + x0;
        if (D.occ[g] || D.label[g] >= 0) continue;
        dist[j] = d + 1; q[tail++] = j;
      }
    }
    const grid = new Grid(w, h);
    for (let i = 0; i < n; i++) if (dist[i] < 0) grid.set(i % w, (i / w) | 0);
    grid.buildSAT();
    let fit = 0;
    for (const vs of probes) {
      let ok = false;
      for (const v of vs) {
        if (v.w > w || v.h > h || v.cells > open) continue;
        for (let y = 0; y + v.h <= h && !ok; y++) for (let x = 0; x + v.w <= w; x++) {
          if (v.w * v.h - grid.boxSum(x, y, x + v.w, y + v.h) < v.cells) continue;
          if (grid.fits(v.pm, x, y)) { ok = true; break; }
        }
        if (ok) break;
      }
      if (ok) fit++;
    }
    return fit / probes.length;
  }
  function chamfer(src, W, H, D) {
    const INF = 1 << 28, n = W * H;
    for (let i = 0; i < n; i++) D[i] = src[i] ? 0 : INF;
    for (let y = 0; y < H; y++) for (let x = 0, i = y * W; x < W; x++, i++) {
      let d = D[i]; if (!d) continue;
      if (x > 0 && D[i - 1] + 3 < d) d = D[i - 1] + 3;
      if (y > 0) { const j = i - W; if (D[j] + 3 < d) d = D[j] + 3; if (x > 0 && D[j - 1] + 4 < d) d = D[j - 1] + 4; if (x < W - 1 && D[j + 1] + 4 < d) d = D[j + 1] + 4; }
      D[i] = d;
    }
    for (let y = H - 1; y >= 0; y--) for (let x = W - 1, i = y * W + W - 1; x >= 0; x--, i--) {
      let d = D[i]; if (!d) continue;
      if (x < W - 1 && D[i + 1] + 3 < d) d = D[i + 1] + 3;
      if (y < H - 1) { const j = i + W; if (D[j] + 3 < d) d = D[j] + 3; if (x < W - 1 && D[j + 1] + 4 < d) d = D[j + 1] + 4; if (x > 0 && D[j - 1] + 4 < d) d = D[j - 1] + 4; }
      D[i] = d;
    }
    return D;
  }
  /** out[i] = 1 for a free cell that no disc of radius rU (thirds of a cell) clear of occupied cells covers. */
  function sliverCells(occ, W, H, rU, A, B, out) {
    const n = W * H;
    chamfer(occ, W, H, A);
    for (let i = 0; i < n; i++) out[i] = A[i] >= rU ? 1 : 0;
    chamfer(out, W, H, B);
    for (let i = 0; i < n; i++) out[i] = !occ[i] && B[i] > rU ? 1 : 0;
    return out;
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
    // A careful append is judged by the space its charms leave unusable, not by how short the strip stays (that
    // squeezed each new charm into the first gap it fitted); with equal counts it also beats a layout without a grade.
    if (Number.isFinite(candidate.fitScore) || Number.isFinite(incumbent.fitScore)) {
      if (!Number.isFinite(incumbent.fitScore)) return true;
      if (!Number.isFinite(candidate.fitScore)) return false;
      if (candidate.fitScore !== incumbent.fitScore) return candidate.fitScore > incumbent.fitScore;
    }
    const a = stripFraction(candidate.placements, sheet), b = stripFraction(incumbent.placements, sheet);
    // Strict ordering prevents epsilon ties from cycling and gradually consuming
    // an offcut as asynchronous workers report higher contact scores.
    if (a !== b) return a < b;
    // Partial Rose Gold sheets: at an equal front, less stock inside the next green line wins.
    if (Number.isFinite(candidate.envelopePt2) && Number.isFinite(incumbent.envelopePt2) && candidate.envelopePt2 !== incumbent.envelopePt2) return candidate.envelopePt2 < incumbent.envelopePt2;
    const contact = x => Number.isFinite(x.contactQuality) ? x.contactQuality : -Infinity;
    if (contact(candidate) !== contact(incumbent)) return contact(candidate) > contact(incumbent);
    const density = x => Number.isFinite(x.density) ? x.density : -Infinity;
    return density(candidate) > density(incumbent);
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

  function unpack(pm) {
    const bits = new Uint8Array(pm.w * pm.h), words = pm.variants[0];
    for (let y = 0; y < pm.h; y++) for (let x = 0; x < pm.w; x++) bits[y * pm.w + x] = (words[y * pm.words + (x >> 5)] >>> (x & 31)) & 1;
    return bits;
  }
  /** Contact against actual silhouette pixels, with distinct piece identities.
   * The one-cell ring rewards minimal remaining clearance more strongly than
   * the wider neighbourhood. Neither sheet walls nor bounding boxes add score. */
  function contactAt(v, fine, x, y) {
    if (!v.touchFine) { const r = ring(v.fine.bits, v.fine.w, v.fine.h, 1); v.touchFine = packShifted(r.bits, r.w, r.h); }
    const material = fine.material;
    if (!material) return {contact:0, close:0, neighbors:0, score:0};
    const pad = v.ringPad;
    let neighbors = 0;
    for (const part of fine.parts || []) {
      if (part.x >= x + v.fine.w + pad || part.x + part.w <= x - pad || part.y >= y + v.fine.h + pad || part.y + part.h <= y - pad) continue;
      if (part.grid.overlap(v.ringFine, x - pad - part.x, y - pad - part.y, 0)) neighbors++;
    }
    if (!neighbors) return {contact:0, close:0, neighbors:0, score:0};
    const contact = material.overlap(v.ringFine, x - pad, y - pad, 1e9) / Math.max(1,v.ringFine.cells);
    const close = material.overlap(v.touchFine, x - 1, y - 1, 1e9) / Math.max(1,v.touchFine.cells);
    return {contact, close, neighbors, score:contact + .5 * close + .05 * neighbors};
  }

  // Edge alignment supports neighbour packing at 0.35 of its contact
  // weight. Snapshot walls exclude charms; boundary contact counts only actual
  // ring pixels, so corner credit stays bounded. Strip growth outranks contact.
  const EDGE_WEIGHT = .35;
  function edgeContact(pm, walls, x, y) {
    if (!walls) return 0;
    let cells = walls.overlap(pm, x, y, 1e9);
    // Count only actual ring pixels beyond the physical stock boundary.
    // Grid.overlap's collision padding deliberately overestimates those rows.
    if (x < 0 || y < 0 || x + pm.w > walls.W || y + pm.h > walls.H) {
      const bits = pm.variants[0];
      for (let r = 0; r < pm.h; r++) {
        const outsideRow = y + r < 0 || y + r >= walls.H;
        if (!outsideRow && x >= 0 && x + pm.w <= walls.W) continue;
        for (let c = 0; c < pm.w; c++) if (outsideRow || x + c < 0 || x + c >= walls.W) cells += (bits[r * pm.words + (c >> 5)] >>> (c & 31)) & 1;
      }
    }
    return cells / Math.max(1, pm.cells);
  }
  // Shape-aware wall fit: a long side running close to a straight cut edge
  // earns more than a protruding hoop or one pointed tip touching that edge.
  // Profiles are cached per rotated mask; no extra AI requests or raster copies.
  const STRAIGHT_EDGE_WEIGHT = .15;
  function sheetBounds(grid) {
    const walls=grid.walls;if(!walls)return {left:0,top:0,right:grid.W,bottom:grid.H};
    if(walls.edgeBounds)return walls.edgeBounds;
    let left=0,top=0,right=grid.W,bottom=grid.H;
    const mx=Math.floor(grid.W/2),my=Math.floor(grid.H/2);
    while(left<right && walls.get(left,my))left++;
    while(right>left && walls.get(right-1,my))right--;
    while(top<bottom && walls.get(mx,top))top++;
    while(bottom>top && walls.get(mx,bottom-1))bottom--;
    return walls.edgeBounds={left,top,right,bottom};
  }
  function cornerPockets(grid,span) {
    const b=sheetBounds(grid),w=Math.min(span,Math.max(1,Math.ceil((b.right-b.left)/2))),h=Math.min(span,Math.max(1,Math.ceil((b.bottom-b.top)/2)));
    grid.buildSAT();
    return [[0,0],[1,0],[0,1],[1,1]].map(([right,bottom])=>{
      const x=right?b.right-w:b.left,y=bottom?b.bottom-h:b.top;
      return {right,bottom,x:right?b.right-1:b.left,y:bottom?b.bottom-1:b.top,free:w*h-grid.boxSum(x,y,x+w,y+h)};
    }).sort((a,b)=>b.free-a.free);
  }
  function straightEdgeAt(mask,grid,x,y,band) {
    const bounds=sheetBounds(grid),gaps=[x-bounds.left,y-bounds.top,bounds.right-x-mask.w,bounds.bottom-y-mask.h];
    if(gaps.every(g=>g>band || g<0))return 0;
    if(!mask.edgeProfile || mask.edgeProfile.band!==band){
      const {w,h}=mask,bits=mask.bits || unpack(mask.pm),depth=[[],[],[],[]];
      for(let row=0;row<h;row++){
        let a=0,b=w-1;while(a<w&&!bits[row*w+a])a++;while(b>=a&&!bits[row*w+b])b--;
        if(a<w){depth[0].push(a);depth[2].push(w-1-b);}
      }
      for(let col=0;col<w;col++){
        let a=0,b=h-1;while(a<h&&!bits[a*w+col])a++;while(b>=a&&!bits[b*w+col])b--;
        if(a<h){depth[1].push(a);depth[3].push(h-1-b);}
      }
      mask.edgeProfile={band,sides:depth.map(ds=>ds.reduce((n,d)=>n+Math.max(0,1-d/(band+1)),0)/Math.max(1,w,h))};
    }
    return Math.max(...gaps.map((gap,i)=>gap<0 || gap>band ? 0 : mask.edgeProfile.sides[i]*(1-gap/(band+1))));
  }
  function edgeBandCells(mask,grid,x,y,band) {
    const {left,top,right,bottom}=sheetBounds(grid);
    if(x>=left+band && y>=top+band && x+mask.w<=right-band && y+mask.h<=bottom-band)return 0;
    const bits=mask.bits || unpack(mask.pm);let cells=0;
    for(let row=0;row<mask.h;row++)for(let col=0;col<mask.w;col++){
      if(bits[row*mask.w+col] && (x+col<left+band || y+row<top+band || x+col>=right-band || y+row>=bottom-band))cells++;
    }
    return cells;
  }
  function placementAt(v, fine, x, y) {
    const adjacency = contactAt(v, fine, x, y), walls = fine.walls;
    const edge = edgeContact(v.ringFine, walls, x - v.ringPad, y - v.ringPad);
    const edgeClose = edgeContact(v.touchFine, walls, x - 1, y - 1);
    const straightEdge=straightEdgeAt(v.fine,fine,x,y,v.ringPad);
    return {...adjacency, neighborScore:adjacency.score, edge, edgeClose, straightEdge, score:adjacency.score + EDGE_WEIGHT * (edge + .5 * edgeClose) + STRAIGHT_EDGE_WEIGHT*straightEdge};
  }

  /** Candidate search for one piece across all its angles. */
  function search(p, fine, coarse, ratio, gravW, noise, random, cornerX, cornerY, strip = null, boundarySeed = false, perimeterPolicy = null, shortlists = null, metrics = null, reportMetrics = null, shortlistOnly = false) {
    const K = 28, TOL = 2;
    // Keep the original construction seed as a competing proposal. Stronger
    // edge alignment is explored on the other trials and on the retained best;
    // it must never force a lower-count layout to replace a denser one.
    const straightWeight = boundarySeed ? 0 : STRAIGHT_EDGE_WEIGHT;
    let best = null;
    const CW = coarse.W, CH = coarse.H, bounds=sheetBounds(coarse);
    const profile=p.guidance?.profiles?.[p.id];
    const partners=profile ? (fine.parts || []).map(part=>({part,weight:packingCompatibility(p.id,part.id,p.guidance)})).filter(x=>x.weight>0).sort((a,b)=>b.weight-a.weight).slice(0,4) : [];
    const pairContact=(v,x,y)=>partners.reduce((n,{part,weight})=>{
      if(part.x>=x+v.fine.w+v.ringPad || part.x+part.w<=x-v.ringPad || part.y>=y+v.fine.h+v.ringPad || part.y+part.h<=y-v.ringPad)return n;
      return Math.max(n,weight*part.grid.overlap(v.ringFine,x-v.ringPad-part.x,y-v.ringPad-part.y,1e9)/Math.max(1,v.ringFine.cells));
    },0);
    const edgeFit=(x,y,w,h)=>{
      if(!profile || profile.edgeRole==="interior")return 0;
      const dx=Math.min(x,fine.W-x-w),dy=Math.min(y,fine.H-y-h),longHorizontal=fine.W>=fine.H;
      const alongLong=longHorizontal ? dy : dx,alongShort=longHorizontal ? dx : dy;
      const gap=profile.edgeRole==="long-edge"?alongLong:profile.edgeRole==="short-edge"?alongShort:profile.edgeRole==="corner"?Math.max(dx,dy):Math.min(dx,dy);
      const parallel=profile.edgeRole==="long-edge" ? (longHorizontal ? w>=h : h>=w) : profile.edgeRole==="short-edge" ? (longHorizontal ? h>=w : w>=h) : true;
      const alignment=parallel ? 1 : Math.min(w,h)/Math.max(1,w,h);
      return profile.edgeAffinity/100*alignment/(1+Math.max(0,gap)/ratio);
    };
    const pairNear=(x,y,w,h)=>partners.reduce((n,{part,weight})=>{const dx=Math.max(0,part.x-x-w,x-part.x-part.w),dy=Math.max(0,part.y-y-h,y-part.y-part.h);return Math.max(n,weight/(1+Math.hypot(dx,dy)/ratio));},0);
    for (const [vi,v] of p.variants.entries()) {
      if(shortlistOnly && !shortlists?.[vi]?.length)continue;
      // ── coarse exhaustive scan ──
      const cands = [], edgeCands=[null,null,null,null];
      reportMetrics?.();
      const cw = v.coarse.w, ch = v.coarse.h, maskCells = v.coarse.pm.cells, boxCells = cw * ch;
      if (!v.coarse.contactRing) { const r = ring(v.coarse.bits || unpack(v.coarse.pm), cw, ch, 1); v.coarse.contactRing = packShifted(r.bits, r.w, r.h); }
      if(shortlists)cands.push(...(shortlists[vi]||[]));
      else for (let y = 0; y + ch <= CH; y++) {
        if((y&7)===0)reportMetrics?.();
        for (let x = 0; x + cw <= CW; x++) {
          if(metrics)metrics.positions++;
          // O(1) box tests first: the mask cannot fit if the box lacks free cells;
          // an empty box needs no bit test at all.
          const inner = coarse.boxSum(x, y, x + cw, y + ch);
          if (boxCells - inner < maskCells - TOL) continue;
          let ov = 0;
          if (inner > 0) { ov = coarse.overlap(v.coarse.pm, x, y, TOL); if (ov > TOL) continue; }
          const near = coarse.material?.boxSum(x - 1, y - 1, x + cw + 1, y + ch + 1);
          const contact = boundarySeed ? (coarse.boxSum(x - 1, y - 1, x + cw + 1, y + ch + 1) - inner) / (2 * (cw + ch) + 4) : near ? coarse.material.overlap(v.coarse.contactRing, x - 1, y - 1, 1e9) / Math.max(1,v.coarse.contactRing.cells) : 0;
          const gx = cornerX ? (CW - x - cw) : x, gy = cornerY ? (CH - y - ch) : y;
          const growth = strip ? Math.max(strip.end / ratio, strip.axis === "x" ? x + cw : y + ch) + (strip.envelope ? LINE_FIT_WEIGHT * envelopeGrowth(strip.envelope.coarse, lineProfile(v.coarse.bits ? v.coarse : Object.assign(v.coarse, { bits: unpack(v.coarse.pm) }), strip.envelope.axis), x, y, strip.envelope.axis) / Math.sqrt(Math.max(1, maskCells)) : 0) : 0;
          const edge = boundarySeed ? 0 : edgeContact(v.coarse.contactRing, coarse.walls, x - 1, y - 1);
          const nearWall=x<=bounds.left+1 || y<=bounds.top+1 || x+cw>=bounds.right-1 || y+ch>=bounds.bottom-1;
          const s = contact + EDGE_WEIGHT * edge + (straightWeight && nearWall ? straightWeight*straightEdgeAt(v.coarse,coarse,x,y,1) : 0) + (profile ? 1.2*pairNear(x*ratio,y*ratio,cw*ratio,ch*ratio)+.15*profile.edgeAffinity/100*edge+.25*edgeFit(x*ratio,y*ratio,cw*ratio,ch*ratio) : 0) - (strip?.weight ?? 4) * growth - gravW * ((gx + gy) / (CW + CH)) - 0.05 * ov + (noise ? noise * random() : 0);
          // Keep the best legal coarse proposal from each wall, even when
          // interior neighbours occupy every slot in the overall shortlist.
          if(!boundarySeed && nearWall){const edgeGaps=[x-bounds.left,y-bounds.top,bounds.right-x-cw,bounds.bottom-y-ch];
            for(let side=0;side<4;side++)if(edgeGaps[side]>=-1 && edgeGaps[side]<=1 && (!edgeCands[side] || s>edgeCands[side].s))edgeCands[side]={x,y,s};
          }
          if (cands.length < K) { cands.push({ x, y, s }); if (cands.length === K) cands.sort((a, b) => b.s - a.s); }
          else if (s > cands[K - 1].s) { cands[K - 1] = { x, y, s }; cands.sort((a, b) => b.s - a.s); }
        }
      }
      cands.push(...edgeCands.filter(Boolean));
      // ── fine refinement around each candidate ──
      const pm = v.fine.pm, FW = fine.W, FH = fine.H, rc = Math.max(1, v.ringFine.cells), pad = v.ringPad;
      const seen = new Set();
      const consider=(x,y)=>{
          if (x < 0 || y < 0 || x + pm.w > FW || y + pm.h > FH) return;
          const key = y * FW + x; if (seen.has(key)) return; seen.add(key);
          if(metrics)metrics.positions++;
          if (!fine.fits(pm, x, y)) return;
          if(perimeterPolicy?.minCells && edgeBandCells(v.fine,fine,x,y,perimeterPolicy.band)<perimeterPolicy.minCells)return;
          const adjacency = placementAt(v, fine, x, y), contact = adjacency.contact;
          const gx = cornerX ? (FW - x - pm.w) : x, gy = cornerY ? (FH - y - pm.h) : y;
          const growth = strip ? Math.max(strip.end, strip.axis === "x" ? x + pm.w : y + pm.h) / ratio + (strip.envelope ? LINE_FIT_WEIGHT * envelopeGrowth(strip.envelope.fine, lineProfile(v.fine, strip.envelope.axis), x, y, strip.envelope.axis) / (ratio * Math.sqrt(Math.max(1, v.cells))) : 0) : 0;
          const s = (boundarySeed ? fine.overlap(v.ringFine, x - pad, y - pad, 1e9) / rc : adjacency.score) + (profile ? 1.2*pairContact(v,x,y)+.15*profile.edgeAffinity/100*adjacency.edge+.25*edgeFit(x,y,pm.w,pm.h) : 0) - (strip?.weight ?? 4) * growth - gravW * ((gx + gy) / (FW + FH)) + (noise ? noise * random() : 0);
          if (!best || s > best.score) best = { v, x, y, score: s, contact, neighbors:adjacency.neighbors, closeContact:adjacency.close };
      };
      for (const c of cands) {
        const x0 = c.x * ratio - ratio, y0 = c.y * ratio - ratio;
        for (let y = y0; y <= y0 + 2 * ratio; y++) for (let x = x0; x <= x0 + 2 * ratio; x++) consider(x,y);
      }
      // A thin, valid edge slot can vanish entirely at coarse resolution.
      // Check every fine-grid position flush with each usable wall directly.
      // Score the ends of each legal run: these nest against its neighbours.
      // Avoid expensive contact grading at thousands of equivalent positions
      // along an empty edge; the coarse shortlist still supplies interior fits.
      const fb=sheetBounds(fine),right=fb.right-pm.w,bottom=fb.bottom-pm.h;
      if(!shortlistOnly && (!boundarySeed || !best) && right>=fb.left && bottom>=fb.top){
        const scanWall=(start,end,fixed,horizontal)=>{
          let run=-1;
          const at=t=>consider(horizontal?t:fixed,horizontal?fixed:t);
          for(let t=start;t<=end+1;t++){
            if(metrics && t<=end)metrics.positions++;
            const legal=t<=end && fine.fits(pm,horizontal?t:fixed,horizontal?fixed:t);
            if(legal && run<0)run=t;
            if(!legal && run>=0){at(run);if(t-1!==run)at(t-1);run=-1;}
          }
        };
        scanWall(fb.left,right,fb.top,true);
        if(bottom!==fb.top)scanWall(fb.left,right,bottom,true);
        scanWall(fb.top,bottom,fb.left,false);
        if(right!==fb.left)scanWall(fb.top,bottom,right,false);
      }
    }
    if (best || shortlistOnly) return best;
    // ── fallback: widen the coarse tolerance so a tight fit is never missed ──
    for (const v of p.variants) {
      const pm = v.fine.pm, FW = fine.W, FH = fine.H, cw = v.coarse.w, ch = v.coarse.h, rc = Math.max(1, v.ringFine.cells), pad = v.ringPad;
      for (let cy = 0; cy + ch <= CH; cy++) {
        if((cy&7)===0)reportMetrics?.();
        for (let cx = 0; cx + cw <= CW; cx++) {
          if(metrics)metrics.positions++;
          if (cw * ch - coarse.boxSum(cx, cy, cx + cw, cy + ch) < v.coarse.pm.cells - 6) continue;
          if (coarse.overlap(v.coarse.pm, cx, cy, 6) > 6) continue;
          for (let y = cy * ratio - ratio; y <= cy * ratio + ratio; y++) for (let x = cx * ratio - ratio; x <= cx * ratio + ratio; x++) {
            if (x < 0 || y < 0 || x + pm.w > FW || y + pm.h > FH) continue;
            if(metrics)metrics.positions++;
            if (!fine.fits(pm, x, y)) continue;
            if(perimeterPolicy?.minCells && edgeBandCells(v.fine,fine,x,y,perimeterPolicy.band)<perimeterPolicy.minCells)continue;
            const adjacency = placementAt(v, fine, x, y), contact = adjacency.contact;
            const growth = strip ? Math.max(strip.end, strip.axis === "x" ? x + pm.w : y + pm.h) / ratio + (strip.envelope ? LINE_FIT_WEIGHT * envelopeGrowth(strip.envelope.fine, lineProfile(v.fine, strip.envelope.axis), x, y, strip.envelope.axis) / (ratio * Math.sqrt(Math.max(1, v.cells))) : 0) : 0;
            const s = (boundarySeed ? fine.overlap(v.ringFine, x - pad, y - pad, 1e9) / rc : adjacency.score) + (profile ? 1.2*pairContact(v,x,y)+.15*profile.edgeAffinity/100*adjacency.edge+.25*edgeFit(x,y,pm.w,pm.h) : 0) - (strip?.weight ?? 4) * growth - gravW * ((x + y) / (FW + FH));
            if (!best || s > best.score) best = { v, x, y, score: s, contact, neighbors:adjacency.neighbors, closeContact:adjacency.close };
          }
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
    let overlapPx = 0, outsidePx = 0, removedPx = 0;
    if(job.sheet.remnant)Rose.validate(job.sheet.remnant,job.sheet.wPt,job.sheet.hPt);
    const locked=new Map([...(job.protectedRose?.placements||[]),...(job.lockedPlacements||[])].map(p=>[p.id,p]));
    let protectedMoved=0;
    if(job.protectedRose)Rose.validate(job.protectedRose.profile,job.sheet.wPt,job.sheet.hPt);
    for(const p of locked.values()){const actual=placements.filter(x=>x.id===p.id);if(actual.length!==1||['cxPt','cyPt','angle'].some(k=>!Number.isFinite(actual[0][k])||Math.abs(actual[0][k]-p[k])>.001)||Math.abs((actual[0]?.scale||1)-(p.scale||1))>.00001)protectedMoved++;}
    const pairs = new Set();
    const masks = [];
    placements.forEach((pl, i) => {
      const p = byId.get(pl.id); if (!p) return;
      const r = resample(p.bits, p.w, p.h, p.scale, res);
      const rot0 = rotateBitmap(r.bits, r.w, r.h, pl.angle);
      const remnant=locked.has(pl.id)?job.sheet.remnant:job.protectedRose?.profile||job.sheet.remnant;
      if(remnant){
        const rx=Math.round(pl.cxPt*res-rot0.cx),ry=Math.round(pl.cyPt*res-rot0.cy);
        for(let y=0;y<rot0.h;y++)for(let x=0;x<rot0.w;x++)if(rot0.bits[y*rot0.w+x]&&Rose.intersects(remnant,(rx+x)/res,(ry+y)/res,1/res,1/res))removedPx++;
      }
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
      ok: overlapPx === 0 && outsidePx === 0 && removedPx === 0 && protectedMoved === 0,
      overlapPx, outsidePx, removedPx, protectedMoved, res,
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
    const erodeFine = clearancePt < 0 ? Math.round(-clearancePt / 2 * fineRes) : 0;
    const wall = Math.round(insetPt * fineRes) + halfGap;
    const g = new Grid(FW, FH);
    for (let y = 0; y < FH; y++) for (let x = 0; x < FW; x++) { if (x < wall || y < wall || x >= FW - wall || y >= FH - wall) g.set(x, y); else g.free[y * FW + x] = 1; }
    if(sheet.remnant){const pad=Math.max(insetPt,halfGap/fineRes);Rose.stamp(g,sheet.remnant,fineRes,pad);g.remnant=remnantFront(sheet.remnant,fineRes,outlinePad(pad,erodeFine,fineRes),FW,FH);}
    stampFixed(g,sheet.fixedPieces,clearancePt,fineRes);
    g.buildSAT(); g.fineRes = fineRes; g.usableCells = g.freeCells();
    return g;
  }
  function stampFixed(grid,entries,clearancePt,res){
    for(const {piece,placement:p} of entries||[]){
      const v=prepareVariant(piece,p.angle,clearancePt,res);
      if(v)grid.stamp(v.fine.bits,v.fine.w,v.fine.h,Math.round(p.cxPt*res-v.solid.cx),Math.round(p.cyPt*res-v.solid.cy));
    }
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
    return { angle, fine: { bits: dil.bits, w: dil.w, h: dil.h, pm: fineMask(dil, rot, erodeFine) }, solid: { w: rot.w, h: rot.h, cx: rot.cx + halfGap - erodeFine, cy: rot.cy + halfGap - erodeFine }, cells: areaOf(rot.bits) };
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
  const solverAPI = { sliverCells, SLIVER_MM, FIT_WEIGHTS, AROUND_MM, search, solve, publicLayout, bestResult, normalizePackingPlan, shapeKey, packingCompatibility, guidedOrderScore, verify, contactAt, placementAt, straightEdgeAt, edgeBandCells, sheetBounds, cornerPockets, betterLayout, stripFraction, erosionPx, rotateBitmap, dilate, erode, ring, resample, packShifted, Grid, rng, popcount32, makeSheetGrid, prepareVariant, tryPlace, tryPlaceTight, stampVariant, bestSpots, coarseFromFine };
  return solverAPI;
});
