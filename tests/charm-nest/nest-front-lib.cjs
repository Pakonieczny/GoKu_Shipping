/* How the front of a sheet lies (Paul, 11 Oct 2026: "I don't like how the GF and SS sheets sometimes create these empty caverns ... nest the
 * pieces in a somewhat straight vertical line front exactly like we already do with the RG 14/20 sheet"): the measure nest-front.cjs and
 * setsim-lib.cjs judge a 100 x 50 mm sheet by (cells are 0.5 pt). (It was the measuring half of nest-front-bench.cjs, a benchmark runner
 * removed 26 Oct 2026; the measure is unchanged.)
 *   frontStd   how straight the front is: the standard deviation, over the rows of the usable band, of how far the farthest charm
 *              reaches on that row (mm; 0 = a straight front)
 *   cavernMm   the widest empty disc (mm across) that fits behind the front: free space bounded by the charms, the sheet's edge band
 *              and the front itself (the 90th percentile of the rows' reach)
 *   blockFill  the share of the stock behind the front that the charms cover                                                        */
'use strict';
const L = require('./pocket-fill-lib.cjs');
const S = L.solverOf(L.REPO), wPt = L.PT(100), hPt = L.PT(50);
const MMC = L.MM / 2;   // one fine cell in mm

/** One sheet's cells: charm bits stamped as the solver stamps them, with the far reach of each row. */
function cellsOf(page) {
  const FW = Math.round(wPt * 2), FH = Math.round(hPt * 2), occ = new Uint8Array(FW * FH), far = new Int32Array(FH), byId = new Map(page.charms.map(c => [c.id, c]));
  const owner = new Int32Array(FW * FH).fill(-1);
  page.placements.forEach((p, k) => {
    const v = S.prepareVariant(byId.get(p.id), p.angle, -.5, 2), x0 = Math.round(p.cxPt * 2 - v.solid.cx), y0 = Math.round(p.cyPt * 2 - v.solid.cy);
    for (let r = 0; r < v.fine.h; r++) for (let q = 0; q < v.fine.w; q++) if (v.fine.bits[r * v.fine.w + q]) { const x = x0 + q, y = y0 + r; if (x >= 0 && y >= 0 && x < FW && y < FH) { occ[y * FW + x] = 1; owner[y * FW + x] = k; if (x + 1 > far[y]) far[y] = x + 1; } }
  });
  return { FW, FH, occ, far, owner };
}
/** Distance (in cells, 3-4 chamfer / 3) from each free cell to the nearest blocked one. */
function distance(blocked, W, H) {
  const d = new Int32Array(W * H), INF = 1e9;
  for (let i = 0; i < W * H; i++) d[i] = blocked[i] ? 0 : INF;
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) { const i = y * W + x; if (!d[i]) continue; let m = d[i];
    if (x > 0) m = Math.min(m, d[i - 1] + 3); if (y > 0) { m = Math.min(m, d[i - W] + 3); if (x > 0) m = Math.min(m, d[i - W - 1] + 4); if (x < W - 1) m = Math.min(m, d[i - W + 1] + 4); } d[i] = m; }
  for (let y = H - 1; y >= 0; y--) for (let x = W - 1; x >= 0; x--) { const i = y * W + x; if (!d[i]) continue; let m = d[i];
    if (x < W - 1) m = Math.min(m, d[i + 1] + 3); if (y < H - 1) { m = Math.min(m, d[i + W] + 3); if (x < W - 1) m = Math.min(m, d[i + W + 1] + 4); if (x > 0) m = Math.min(m, d[i + W - 1] + 4); } d[i] = m; }
  return d;
}
/** The front of a sheet and the empty space behind it (see the header). */
function front(page) {
  const { FW, FH, occ, far } = cellsOf(page), band = 3, rows = [];
  for (let y = band; y < FH - band; y++) rows.push(far[y]);
  const mm = c => c * MMC, mean = rows.reduce((a, b) => a + b, 0) / rows.length, std = Math.sqrt(rows.reduce((a, b) => a + (b - mean) ** 2, 0) / rows.length);
  const sorted = rows.slice().sort((a, b) => a - b), xf = Math.max(band + 1, sorted[Math.floor(.9 * (sorted.length - 1))]);
  // blocked: outside the usable band, any charm, and everything beyond the front
  const blocked = new Uint8Array(FW * FH); let free = 0, covered = 0;
  for (let y = 0; y < FH; y++) for (let x = 0; x < FW; x++) {
    const usable = x >= band && x < FW - band && y >= band && y < FH - band, behind = x < xf;
    if (!usable || !behind || occ[y * FW + x]) blocked[y * FW + x] = 1;
    if (usable && behind) { if (occ[y * FW + x]) covered++; else free++; }
  }
  const d = distance(blocked, FW, FH); let mx = 0; for (let i = 0; i < d.length; i++) if (d[i] > mx) mx = d[i];
  return { frontStd: +mm(std).toFixed(2), meanFar: +mm(mean).toFixed(1), maxFar: +mm(sorted[sorted.length - 1]).toFixed(1), minFar: +mm(sorted[0]).toFixed(1), frontP90: +mm(xf).toFixed(1), cavernMm: +(2 * mm(mx / 3)).toFixed(1), blockFill: +(covered / Math.max(1, covered + free)).toFixed(3) };
}

module.exports = { front };
