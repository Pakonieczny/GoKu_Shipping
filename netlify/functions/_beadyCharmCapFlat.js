'use strict';
/*
 * capFlatSlot(outputBuf, templateBuf, { slotIndex, factor })
 *
 * Deterministically shrinks the pendant charm(s) drawn on the two FLAT Beady-Necklace
 * listing graphics after AI generation:
 *   slotIndex 2  (Charm_Size_Guide) : one charm, scaled about the centre of its bbox
 *   slotIndex 3  (Back_Engraving)   : two charms (gold front, silver back), each scaled about
 *                                     the top-centre of its hoop (where the jump ring passes)
 * Everything outside the charm's (dilated) footprint stays byte-identical; the vacated area is
 * painted with the flat background colour. Never throws for image-related problems: on any
 * doubt it returns { buf: outputBuf (same object), changed:false, reason }.
 */
const sharp = require('sharp');

const DEFAULT_FACTOR = 0.87;

// ---------------------------------------------------------------- constants
const C = {
  T_EDGE: 4,          // dist>=4 (max-channel |pixel-bg|) => "not background". bg noise is <=2 (p99.9), shadows/halos are 1..8.
  T_CORE: 10,         // dist>=10 => solid charm pixel; used only for bbox measurements (excludes soft halo)
  T_HALO: 3,          // hysteresis threshold used to grow the erase zone along long/soft shadows
  CLOSE_R: 4,         // px @2048: closes hairline gaps in light (silver) outlines before hole filling
  HALO_R: 0.0065,     // erase-zone dilation as fraction of W (13 px @2048): covers the measured 10-15 px soft halo
  HALO_MAX: 0.022,    // hysteresis growth limit as fraction of W (45 px @2048)
  MIN_BG_FLAT: 0.55,  // min fraction of sampled pixels within dist<=4 of the background colour
  RATIO_TOL: 0.04,    // post-check: measured bbox ratio must be within +-4% (relative) of factor
  EPS_PCT_CLAMP: 255
};

// Slot 3 layout (fractions of the 2048 canvas; measured on the template + 12 generated outputs)
const S3 = {
  ROI: [0.365, 0.25, 0.70, 0.58],        // x0,y0,x1,y1: right of the pencil (eraser ends at 0.353), below the title (ends 0.175), above the caption (starts 0.60)
  PTR_WIN: [0.42, 0.47, 0.62, 0.60],     // where the pointer line can be
  PTR_MAX_W: 0.012, PTR_MIN_H: 0.025,    // pointer: thin (<=24px) and >=51px tall
  CX: [0.44, 0.60], CY: [0.27, 0.56],    // plausible bbox-centre range
  MIN_SIDE: 0.03, MAX_SIDE: 0.30         // plausible bbox side range
};
// Slot 4 layout: jump-ring x and the row (a few px above the hoop top) where the charm starts
const S4 = {
  gold:   { x: 818 / 2048,  cut: 996 / 2048 },
  silver: { x: 1190 / 2048, cut: 1068 / 2048 },
  HALF_W: 0.13,           // window half width around the ring
  WIN_H: 0.21,            // window height below the cut
  GUARD_HALF: 0.0068,     // half width of the ring guard band (14 px)
  GUARD_H: 4,             // guard rows above the cut used only as resampling source
  MIN_W: 0.03, MAX_W: 0.26, MIN_H: 0.06, MAX_H: 0.24
};

// ---------------------------------------------------------------- small utils
const clamp = (v, a, b) => (v < a ? a : v > b ? b : v);
const skip = (outputBuf, reason, info) => ({ buf: outputBuf, changed: false, reason, info: info || {} });

// Felzenszwalb squared Euclidean distance transform: distance^2 to nearest feature(=1) pixel
function distSq(feat, w, h) {
  const INF = 1e12;
  const n = Math.max(w, h);
  const f = new Float64Array(n), d = new Float64Array(n), z = new Float64Array(n + 1), v = new Int32Array(n);
  const out = new Float32Array(w * h);
  const col = new Float64Array(w * h);
  // columns first
  for (let x = 0; x < w; x++) {
    for (let y = 0; y < h; y++) f[y] = feat[y * w + x] ? 0 : INF;
    dt1d(f, d, h, v, z);
    for (let y = 0; y < h; y++) col[y * w + x] = d[y];
  }
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) f[x] = col[y * w + x];
    dt1d(f, d, w, v, z);
    for (let x = 0; x < w; x++) out[y * w + x] = d[x];
  }
  return out;
}
function dt1d(f, d, n, v, z) {
  const INF = 1e20;
  let k = 0; v[0] = 0; z[0] = -INF; z[1] = INF;
  for (let q = 1; q < n; q++) {
    let s = ((f[q] + q * q) - (f[v[k]] + v[k] * v[k])) / (2 * q - 2 * v[k]);
    while (s <= z[k]) { k--; s = ((f[q] + q * q) - (f[v[k]] + v[k] * v[k])) / (2 * q - 2 * v[k]); }
    k++; v[k] = q; z[k] = s; z[k + 1] = INF;
  }
  k = 0;
  for (let q = 0; q < n; q++) {
    while (z[k + 1] < q) k++;
    const dq = q - v[k];
    d[q] = dq * dq + f[v[k]];
  }
}
function dilate(m, w, h, r) {
  if (r <= 0) return Uint8Array.from(m);
  const d = distSq(m, w, h), r2 = r * r + 1e-6, o = new Uint8Array(w * h);
  for (let i = 0; i < o.length; i++) o[i] = d[i] <= r2 ? 1 : 0;
  return o;
}
function erode(m, w, h, r) { // outside the window counts as "set"
  if (r <= 0) return Uint8Array.from(m);
  const inv = new Uint8Array(w * h);
  for (let i = 0; i < inv.length; i++) inv[i] = m[i] ? 0 : 1;
  const dl = dilate(inv, w, h, r), o = new Uint8Array(w * h);
  for (let i = 0; i < o.length; i++) o[i] = dl[i] ? 0 : 1;
  return o;
}
function closing(m, w, h, r) { return erode(dilate(m, w, h, r), w, h, r); }
function fillHoles(m, w, h) {
  const reach = new Uint8Array(w * h), st = new Int32Array(w * h);
  let sp = 0;
  const push = (i) => { if (!m[i] && !reach[i]) { reach[i] = 1; st[sp++] = i; } };
  for (let x = 0; x < w; x++) { push(x); push((h - 1) * w + x); }
  for (let y = 0; y < h; y++) { push(y * w); push(y * w + w - 1); }
  while (sp) {
    const i = st[--sp], x = i % w, y = (i / w) | 0;
    if (x > 0) push(i - 1);
    if (x < w - 1) push(i + 1);
    if (y > 0) push(i - w);
    if (y < h - 1) push(i + w);
  }
  const o = new Uint8Array(w * h);
  for (let i = 0; i < o.length; i++) o[i] = (m[i] || !reach[i]) ? 1 : 0;
  return o;
}
// 8-connected labelling. returns { lab:Int32Array, n, comps:[{id,area,x0,y0,x1,y1}] } (ids from 1)
function label8(m, w, h) {
  const lab = new Int32Array(w * h), st = new Int32Array(w * h), comps = [];
  let n = 0;
  for (let s = 0; s < w * h; s++) {
    if (!m[s] || lab[s]) continue;
    n++;
    let sp = 0; st[sp++] = s; lab[s] = n;
    let area = 0, x0 = w, y0 = h, x1 = -1, y1 = -1;
    while (sp) {
      const i = st[--sp], x = i % w, y = (i / w) | 0;
      area++;
      if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y;
      for (let dy = -1; dy <= 1; dy++) {
        const yy = y + dy; if (yy < 0 || yy >= h) continue;
        for (let dx = -1; dx <= 1; dx++) {
          const xx = x + dx; if (xx < 0 || xx >= w) continue;
          const j = yy * w + xx;
          if (m[j] && !lab[j]) { lab[j] = n; st[sp++] = j; }
        }
      }
    }
    comps.push({ id: n, area, x0, y0, x1, y1 });
  }
  return { lab, n, comps };
}

// ---------------------------------------------------------------- image context
async function decode(buf) {
  const img = sharp(buf, { failOn: 'none', limitInputPixels: false });
  const meta = await img.metadata();
  if (!meta.width || !meta.height) throw new Error('nodim');
  if (meta.depth && meta.depth !== 'uchar') throw new Error('depth');
  const { data, info } = await img.toColourspace('srgb').raw().toBuffer({ resolveWithObject: true });
  if (info.channels !== 3 && info.channels !== 4) throw new Error('channels');
  return { data, w: info.width, h: info.height, ch: info.channels };
}
function estimateBg(ctx) {
  const { data, w, h, ch } = ctx;
  const hist = [new Uint32Array(256), new Uint32Array(256), new Uint32Array(256)];
  let n = 0;
  for (let y = 0; y < h; y += 4) for (let x = 0; x < w; x += 4) {
    const i = (y * w + x) * ch;
    hist[0][data[i]]++; hist[1][data[i + 1]]++; hist[2][data[i + 2]]++; n++;
  }
  const bg = [0, 0, 0];
  for (let c = 0; c < 3; c++) { let a = 0; for (let v = 0; v < 256; v++) { a += hist[c][v]; if (a * 2 >= n) { bg[c] = v; break; } } }
  let flat = 0;
  for (let y = 0; y < h; y += 4) for (let x = 0; x < w; x += 4) {
    const i = (y * w + x) * ch;
    if (Math.max(Math.abs(data[i] - bg[0]), Math.abs(data[i + 1] - bg[1]), Math.abs(data[i + 2] - bg[2])) <= C.T_EDGE) flat++;
  }
  return { bg, flat: flat / n };
}
// max-channel distance to bg for a window
function distWin(ctx, x0, y0, ww, wh) {
  const { data, w, ch, bg } = ctx;
  const o = new Uint8Array(ww * wh);
  for (let y = 0; y < wh; y++) {
    let i = ((y0 + y) * w + x0) * ch, j = y * ww;
    for (let x = 0; x < ww; x++, i += ch, j++) {
      const a = Math.abs(data[i] - bg[0]), b = Math.abs(data[i + 1] - bg[1]), c = Math.abs(data[i + 2] - bg[2]);
      o[j] = a > b ? (a > c ? a : c) : (b > c ? b : c);
    }
  }
  return o;
}

// ---------------------------------------------------------------- template gate
function blockMeans(data, w, h, ch, k) {
  const bw = Math.floor(w / k), bh = Math.floor(h / k), o = new Float32Array(bw * bh * 3);
  for (let by = 0; by < bh; by++) for (let bx = 0; bx < bw; bx++) {
    let r = 0, g = 0, b = 0;
    for (let y = 0; y < k; y++) { let i = ((by * k + y) * w + bx * k) * ch; for (let x = 0; x < k; x++, i += ch) { r += data[i]; g += data[i + 1]; b += data[i + 2]; } }
    const j = (by * bw + bx) * 3, nn = k * k; o[j] = r / nn; o[j + 1] = g / nn; o[j + 2] = b / nn;
  }
  return { o, bw, bh };
}
async function templateGate(ctx, templateBuf, slot) {
  if (!templateBuf || !templateBuf.length) return { ok: false, reason: 'no template to check the layout against' };
  let t;
  try {
    const { data, info } = await sharp(templateBuf, { failOn: 'none', limitInputPixels: false })
      .removeAlpha().toColourspace('srgb').resize(ctx.w, ctx.h, { fit: 'fill', kernel: 'lanczos3' }).raw().toBuffer({ resolveWithObject: true });
    t = { data, w: info.width, h: info.height, ch: info.channels };
  } catch (e) { return { ok: false, reason: 'template undecodable' }; }
  const meta = await sharp(templateBuf, { failOn: 'none', limitInputPixels: false }).metadata();
  if (Math.abs(meta.width / meta.height - ctx.w / ctx.h) > 0.02) return { ok: false, reason: 'template aspect mismatch' };
  const k = 16;
  const a = blockMeans(ctx.data, ctx.w, ctx.h, ctx.ch, k), b = blockMeans(t.data, t.w, t.h, t.ch, k);
  // zones the charm can never touch
  const zones = slot === 2 ? [[0, 0.24, 0, 1], [0.62, 1, 0, 1], [0.30, 0.60, 0, 0.34]] : [[0.74, 1, 0, 1], [0, 0.42, 0, 1]];
  let s = 0, n = 0;
  for (const [y0, y1, x0, x1] of zones) {
    for (let by = Math.floor(y0 * a.bh); by < Math.floor(y1 * a.bh); by++) for (let bx = Math.floor(x0 * a.bw); bx < Math.floor(x1 * a.bw); bx++) {
      const j = (by * a.bw + bx) * 3;
      s += (Math.abs(a.o[j] - b.o[j]) + Math.abs(a.o[j + 1] - b.o[j + 1]) + Math.abs(a.o[j + 2] - b.o[j + 2])) / 3; n++;
    }
  }
  const mad = s / Math.max(1, n);
  // template background colour must match (both are flat cream/white)
  const bgT = estimateBg({ data: t.data, w: t.w, h: t.h, ch: t.ch });
  const bgd = Math.max(Math.abs(bgT.bg[0] - ctx.bg[0]), Math.abs(bgT.bg[1] - ctx.bg[1]), Math.abs(bgT.bg[2] - ctx.bg[2]));
  if (mad > 8) return { ok: false, reason: 'template layout mismatch (mad ' + mad.toFixed(1) + ')' };
  if (bgd > 14) return { ok: false, reason: 'template background mismatch' };
  return { ok: true, mad, bgd };
}

// ---------------------------------------------------------------- erase-zone (footprint) builder
// G: silhouette mask (window coords). prot: never-touch mask. limitWin: allowed area mask (or null).
function buildErase(dist, G, prot, ww, wh, rHalo, rMax, allowed) {
  const dG = distSq(G, ww, wh), le = (r) => { const o = new Uint8Array(ww * wh), r2 = r * r + 1e-6; for (let i = 0; i < o.length; i++) o[i] = dG[i] <= r2 ? 1 : 0; return o; };
  let E = le(rHalo);
  // hysteresis growth along soft/long shadows: pixels >= T_HALO connected to E, within rMax of G
  const Gd = le(rMax), Eseed = le(2);
  const H0 = new Uint8Array(ww * wh);
  for (let i = 0; i < H0.length; i++) H0[i] = (dist[i] >= C.T_HALO && Gd[i] && !prot[i] && (!allowed || allowed[i])) ? 1 : 0;
  const L = label8(H0, ww, wh);
  const keep = new Uint8Array(L.n + 1);
  for (let i = 0; i < H0.length; i++) if (H0[i] && (Eseed[i] || E[i])) keep[L.lab[i]] = 1;
  for (let i = 0; i < H0.length; i++) if (H0[i] && keep[L.lab[i]]) E[i] = 1;
  E = closing(E, ww, wh, 5);
  E = fillHoles(E, ww, wh);
  for (let i = 0; i < E.length; i++) if (prot[i] || (allowed && !allowed[i])) E[i] = 0;
  return E;
}

// ---------------------------------------------------------------- resampling & compositing
function lanczos3(x) {
  if (x === 0) return 1;
  if (x <= -3 || x >= 3) return 0;
  const px = Math.PI * x;
  return (3 * Math.sin(px) * Math.sin(px / 3)) / (px * px);
}
// weights for scaling by f about anchor a (continuous coords): dest pixel X (abs) samples source c = a + (X+0.5-a)/f
function makeWeights(dst0, dstLen, src0, srcLen, a, f) {
  const sup = 3 / f, taps = new Array(dstLen), first = new Int32Array(dstLen), cnt = new Int32Array(dstLen);
  for (let j = 0; j < dstLen; j++) {
    const c = a + (dst0 + j + 0.5 - a) / f;
    const i0 = Math.ceil(c - sup - 0.5), i1 = Math.floor(c + sup - 0.5);
    const ws = new Float64Array(i1 - i0 + 1);
    let sum = 0;
    for (let i = i0; i <= i1; i++) { const wv = lanczos3((i + 0.5 - c) * f); ws[i - i0] = wv; sum += wv; }
    // normalise over ALL taps; taps outside the source rectangle carry zero signal (delta is 0 there)
    const lo = Math.max(i0, src0), hi = Math.min(i1, src0 + srcLen - 1);
    const arr = new Float32Array(Math.max(0, hi - lo + 1));
    for (let i = lo; i <= hi; i++) arr[i - lo] = ws[i - i0] / sum;
    first[j] = lo - src0; cnt[j] = arr.length; taps[j] = arr;
  }
  return { taps, first, cnt };
}
/*
 * Shrinks the "difference from background" image of the footprint E about (ax,ay) by f and writes
 * bg + shrunk difference into `out`. Pixels in prot are never written.
 * Returns bbox of written pixels {x0,y0,x1,y1} (absolute) or null.
 */
function shrinkInto(ctx, out, win, E, guard, prot, ax, ay, f) {
  const { data, w, h, ch, bg } = ctx;
  const ww = win.w, wh = win.h;
  // source bbox (E + guard)
  let sx0 = ww, sy0 = wh, sx1 = -1, sy1 = -1;
  for (let y = 0; y < wh; y++) for (let x = 0; x < ww; x++) {
    const i = y * ww + x;
    if (E[i] || (guard && guard[i])) { if (x < sx0) sx0 = x; if (x > sx1) sx1 = x; if (y < sy0) sy0 = y; if (y > sy1) sy1 = y; }
  }
  if (sx1 < 0) return null;
  const SX0 = win.x + sx0, SY0 = win.y + sy0, SW = sx1 - sx0 + 1, SH = sy1 - sy0 + 1;
  // destination region: union of the source bbox (so the whole old footprint is repainted) and its contraction (+3px filter spill)
  const dX0 = Math.max(win.x, Math.min(SX0, Math.floor(ax + (SX0 - ax) * f) - 3)), dX1 = Math.min(win.x + ww - 1, Math.max(SX0 + SW - 1, Math.ceil(ax + (SX0 + SW - ax) * f) + 3));
  const dY0 = Math.max(win.y, Math.min(SY0, Math.floor(ay + (SY0 - ay) * f) - 3)), dY1 = Math.min(win.y + wh - 1, Math.max(SY0 + SH - 1, Math.ceil(ay + (SY0 + SH - ay) * f) + 3));
  const DW = dX1 - dX0 + 1, DH = dY1 - dY0 + 1;
  // source planes: 3 delta channels + footprint indicator
  const src = new Float32Array(SW * SH * 4);
  for (let y = 0; y < SH; y++) for (let x = 0; x < SW; x++) {
    const wi = (sy0 + y) * ww + (sx0 + x);
    if (!(E[wi] || (guard && guard[wi]))) continue;
    const pi = ((SY0 + y) * w + (SX0 + x)) * ch, o = (y * SW + x) * 4;
    src[o] = data[pi] - bg[0]; src[o + 1] = data[pi + 1] - bg[1]; src[o + 2] = data[pi + 2] - bg[2];
    src[o + 3] = E[wi] ? 1 : 0;
  }
  const wx = makeWeights(dX0, DW, SX0, SW, ax, f), wy = makeWeights(dY0, DH, SY0, SH, ay, f);
  // horizontal pass
  const tmp = new Float32Array(SH * DW * 4);
  for (let y = 0; y < SH; y++) for (let j = 0; j < DW; j++) {
    const tp = wx.taps[j], f0 = wx.first[j], n = wx.cnt[j];
    let a0 = 0, a1 = 0, a2 = 0, a3 = 0;
    for (let q = 0; q < n; q++) { const o = (y * SW + f0 + q) * 4, wv = tp[q]; a0 += src[o] * wv; a1 += src[o + 1] * wv; a2 += src[o + 2] * wv; a3 += src[o + 3] * wv; }
    const o = (y * DW + j) * 4; tmp[o] = a0; tmp[o + 1] = a1; tmp[o + 2] = a2; tmp[o + 3] = a3;
  }
  // vertical pass + write
  let wb = null;
  for (let j = 0; j < DH; j++) {
    const tp = wy.taps[j], f0 = wy.first[j], n = wy.cnt[j];
    const Y = dY0 + j, wyIdx = Y - win.y;
    for (let x = 0; x < DW; x++) {
      let a0 = 0, a1 = 0, a2 = 0, a3 = 0;
      for (let q = 0; q < n; q++) { const o = ((f0 + q) * DW + x) * 4, wv = tp[q]; a0 += tmp[o] * wv; a1 += tmp[o + 1] * wv; a2 += tmp[o + 2] * wv; a3 += tmp[o + 3] * wv; }
      const X = dX0 + x, wi = wyIdx * ww + (X - win.x);
      if (prot[wi]) continue;
      // write set: original footprint, or footprint of the shrunk charm (with a hair of filter spill)
      if (!(E[wi] || a3 > 0.002)) continue;
      const oi = (Y * w + X) * ch;
      out[oi] = clamp(Math.round(bg[0] + a0), 0, 255);
      out[oi + 1] = clamp(Math.round(bg[1] + a1), 0, 255);
      out[oi + 2] = clamp(Math.round(bg[2] + a2), 0, 255);
      if (!wb) wb = { x0: X, y0: Y, x1: X, y1: Y };
      if (X < wb.x0) wb.x0 = X; if (X > wb.x1) wb.x1 = X; if (Y < wb.y0) wb.y0 = Y; if (Y > wb.y1) wb.y1 = Y;
    }
  }
  // the original footprint pixels that were written are also part of the touched bbox
  for (let y = 0; y < wh; y++) for (let x = 0; x < ww; x++) if (E[y * ww + x]) {
    const X = win.x + x, Y = win.y + y;
    if (!wb) wb = { x0: X, y0: Y, x1: X, y1: Y };
    if (X < wb.x0) wb.x0 = X; if (X > wb.x1) wb.x1 = X; if (Y < wb.y0) wb.y0 = Y; if (Y > wb.y1) wb.y1 = Y;
  }
  return wb;
}

// ---------------------------------------------------------------- slot 3 (sizing guide)
function findPointer(ctx) {
  const { data, w, h, ch } = ctx;
  const x0 = Math.round(S3.PTR_WIN[0] * w), y0 = Math.round(S3.PTR_WIN[1] * h), x1 = Math.round(S3.PTR_WIN[2] * w), y1 = Math.round(S3.PTR_WIN[3] * h);
  const ww = x1 - x0, wh = y1 - y0, m = new Uint8Array(ww * wh);
  for (let y = 0; y < wh; y++) for (let x = 0; x < ww; x++) {
    const i = ((y0 + y) * w + x0 + x) * ch, r = data[i], g = data[i + 1], b = data[i + 2];
    const mx = Math.max(r, g, b), mn = Math.min(r, g, b);
    if ((r + g + b) / 3 < 130 && mx - mn <= 36) m[y * ww + x] = 1;
  }
  const L = label8(m, ww, wh);
  let best = null;
  for (const c of L.comps) {
    const cw = c.x1 - c.x0 + 1, chh = c.y1 - c.y0 + 1;
    if (cw <= S3.PTR_MAX_W * w && chh >= S3.PTR_MIN_H * h && chh >= 4 * cw && c.area >= 0.5 * cw * chh) { if (!best || chh > best.hh) best = { x0: x0 + c.x0, x1: x0 + c.x1, y0: y0 + c.y0, y1: y0 + c.y1, hh: chh }; }
  }
  return best;
}
// returns { ok, reason, ... } describing the charm in the sizing guide
function detectSlot3(ctx) {
  const { w, h } = ctx;
  const rx0 = Math.round(S3.ROI[0] * w), ry0 = Math.round(S3.ROI[1] * h), rx1 = Math.round(S3.ROI[2] * w), ry1 = Math.round(S3.ROI[3] * h);
  const ww = rx1 - rx0, wh = ry1 - ry0;
  const ptr = findPointer(ctx);
  if (!ptr) return { ok: false, reason: 'pointer line not found' };
  const prot = new Uint8Array(ww * wh);
  const px0 = ptr.x0 - 4 - rx0, px1 = ptr.x1 + 4 - rx0, py0 = ptr.y0 - 1 - ry0, py1 = ptr.y1 + 4 - ry0;
  for (let y = Math.max(0, py0); y <= Math.min(wh - 1, py1); y++) for (let x = Math.max(0, px0); x <= Math.min(ww - 1, px1); x++) prot[y * ww + x] = 1;
  const dist = distWin(ctx, rx0, ry0, ww, wh);
  const M = new Uint8Array(ww * wh);
  for (let i = 0; i < M.length; i++) M[i] = (dist[i] >= C.T_EDGE && !prot[i]) ? 1 : 0;
  let F = closing(M, ww, wh, Math.round(C.CLOSE_R * w / 2048));
  F = fillHoles(F, ww, wh);
  for (let i = 0; i < F.length; i++) if (prot[i]) F[i] = 0;
  const L = label8(F, ww, wh);
  if (!L.comps.length) return { ok: false, reason: 'charm not found' };
  const main = L.comps.reduce((a, b) => (b.area > a.area ? b : a));
  if (main.area < 0.0006 * w * h) return { ok: false, reason: 'charm too small / not found' };
  const keep = L.comps.filter((c) => c.area >= Math.max(150, 0.02 * main.area));
  if (keep.length > 4) return { ok: false, reason: 'too many components (' + keep.length + ')' };
  const keepIds = new Set(keep.map((c) => c.id));
  const G = new Uint8Array(ww * wh);
  for (let i = 0; i < G.length; i++) if (keepIds.has(L.lab[i])) G[i] = 1;
  // touches ROI border?
  for (const c of keep) if (c.x0 <= 1 || c.y0 <= 1 || c.x1 >= ww - 2 || c.y1 >= wh - 2) return { ok: false, reason: 'charm touches search-area border' };
  // core bbox (solid pixels only)
  let bx0 = ww, by0 = wh, bx1 = -1, by1 = -1;
  for (let y = 0; y < wh; y++) for (let x = 0; x < ww; x++) { const i = y * ww + x; if (G[i] && dist[i] >= C.T_CORE) { if (x < bx0) bx0 = x; if (x > bx1) bx1 = x; if (y < by0) by0 = y; if (y > by1) by1 = y; } }
  if (bx1 < 0) return { ok: false, reason: 'no solid charm pixels' };
  const bbox = { x0: rx0 + bx0, y0: ry0 + by0, x1: rx0 + bx1, y1: ry0 + by1 };
  const bw = bbox.x1 - bbox.x0 + 1, bh = bbox.y1 - bbox.y0 + 1;
  if (bw < S3.MIN_SIDE * w || bh < S3.MIN_SIDE * h || bw > S3.MAX_SIDE * w || bh > S3.MAX_SIDE * h) return { ok: false, reason: 'implausible charm size ' + bw + 'x' + bh };
  const cx = (bbox.x0 + bbox.x1 + 1) / 2, cy = (bbox.y0 + bbox.y1 + 1) / 2;
  if (cx < S3.CX[0] * w || cx > S3.CX[1] * w || cy < S3.CY[0] * h || cy > S3.CY[1] * h) return { ok: false, reason: 'implausible charm position' };
  if (Math.abs(cx - (ptr.x0 + ptr.x1 + 1) / 2) > 0.09 * w) return { ok: false, reason: 'charm not above pointer' };
  return { ok: true, win: { x: rx0, y: ry0, w: ww, h: wh }, dist, G, prot, bbox, cx, cy, ptr, ncomp: keep.length };
}
function processSlot3(ctx, f) {
  const det = detectSlot3(ctx);
  if (!det.ok) return det;
  const { win, dist, G, prot } = det;
  const rH = Math.round(C.HALO_R * ctx.w), rM = Math.round(C.HALO_MAX * ctx.w);
  const E = buildErase(dist, G, prot, win.w, win.h, rH, rM, null);
  const out = Buffer.from(ctx.data);
  const wb = shrinkInto(ctx, out, win, E, null, prot, det.cx, det.cy, f);
  return { ok: true, out, wb, det, bboxes: [{ name: 'charm', before: det.bbox }], anchors: [{ x: det.cx, y: det.cy }] };
}

// ---------------------------------------------------------------- slot 4 (back engraving)
function detectCharm4(ctx, which) {
  const { w, h } = ctx;
  const A = S4[which], other = S4[which === 'gold' ? 'silver' : 'gold'];
  const ax = A.x * w, cut = Math.round(A.cut * h), mid = Math.round((A.x + other.x) / 2 * w);
  const hw = Math.round(S4.HALF_W * w);
  let wx0, wx1;
  if (which === 'gold') { wx0 = Math.round(ax - hw); wx1 = mid - 3; } else { wx0 = mid + 3; wx1 = Math.round(ax + hw); }
  const wy0 = cut, wy1 = Math.min(h - 1, cut + Math.round(S4.WIN_H * h));
  const ww = wx1 - wx0 + 1, wh = wy1 - wy0 + 1;
  const dist = distWin(ctx, wx0, wy0, ww, wh);
  const M = new Uint8Array(ww * wh);
  for (let i = 0; i < M.length; i++) M[i] = dist[i] >= C.T_EDGE ? 1 : 0;
  let F = closing(M, ww, wh, Math.round(C.CLOSE_R * w / 2048));
  F = fillHoles(F, ww, wh);
  const L = label8(F, ww, wh);
  // component sitting on the jump ring just below the cut
  const rx = Math.round(ax) - wx0, hs = Math.round(0.007 * w);
  const votes = new Map();
  for (let y = 0; y < Math.min(wh, 14); y++) for (let x = Math.max(0, rx - hs); x <= Math.min(ww - 1, rx + hs); x++) { const id = L.lab[y * ww + x]; if (id) votes.set(id, (votes.get(id) || 0) + 1); }
  if (!votes.size) return { ok: false, reason: which + ': no ring/charm at anchor' };
  let mainId = 0, mv = -1; for (const [id, v] of votes) if (v > mv) { mv = v; mainId = id; }
  const main = L.comps[mainId - 1];
  // other pieces close to the main piece belong to the charm; big far pieces mean a crowded/odd scene
  const mainMask = new Uint8Array(ww * wh); for (let i = 0; i < mainMask.length; i++) if (L.lab[i] === mainId) mainMask[i] = 1;
  const near = dilate(mainMask, ww, wh, Math.round(0.02 * w));
  const ids = new Set([mainId]);
  for (const c of L.comps) {
    if (c.id === mainId || c.area < 60) continue;
    let touch = false;
    for (let y = c.y0; y <= c.y1 && !touch; y++) for (let x = c.x0; x <= c.x1; x++) if (L.lab[y * ww + x] === c.id && near[y * ww + x]) { touch = true; break; }
    if (touch) ids.add(c.id); else if (c.area >= 0.03 * main.area && c.area >= 200) return { ok: false, reason: which + ': unrelated object near charm' };
  }
  const G = new Uint8Array(ww * wh);
  let ux0 = ww, uy0 = wh, ux1 = -1, uy1 = -1;
  for (let y = 0; y < wh; y++) for (let x = 0; x < ww; x++) { const i = y * ww + x; if (ids.has(L.lab[i])) { G[i] = 1; if (x < ux0) ux0 = x; if (x > ux1) ux1 = x; if (y < uy0) uy0 = y; if (y > uy1) uy1 = y; } }
  if (ux0 <= 1 || ux1 >= ww - 2 || uy1 >= wh - 2) return { ok: false, reason: which + ': charm touches search-area border' };
  // core bbox
  let bx0 = ww, by0 = wh, bx1 = -1, by1 = -1;
  for (let y = 0; y < wh; y++) for (let x = 0; x < ww; x++) { const i = y * ww + x; if (G[i] && dist[i] >= C.T_CORE) { if (x < bx0) bx0 = x; if (x > bx1) bx1 = x; if (y < by0) by0 = y; if (y > by1) by1 = y; } }
  if (bx1 < 0) return { ok: false, reason: which + ': no solid pixels' };
  const bbox = { x0: wx0 + bx0, y0: wy0 + by0, x1: wx0 + bx1, y1: wy0 + by1 };
  const bw = bbox.x1 - bbox.x0 + 1, bh = bbox.y1 - bbox.y0 + 1;
  if (bw < S4.MIN_W * w || bw > S4.MAX_W * w || bh < S4.MIN_H * h || bh > S4.MAX_H * h) return { ok: false, reason: which + ': implausible size ' + bw + 'x' + bh };
  if (ax < bbox.x0 + 0.1 * bw || ax > bbox.x1 - 0.1 * bw) return { ok: false, reason: which + ': ring not over charm' };
  // protect everything above the cut is implicit (window starts at the cut)
  const prot = new Uint8Array(ww * wh);
  return { ok: true, which, win: { x: wx0, y: wy0, w: ww, h: wh }, dist, G, prot, bbox, ax, cut, ncomp: ids.size };
}
function processSlot4(ctx, f) {
  const dets = [];
  for (const which of ['gold', 'silver']) { const d = detectCharm4(ctx, which); if (!d.ok) return d; dets.push(d); }
  const out = Buffer.from(ctx.data), rH = Math.round(C.HALO_R * ctx.w), rM = Math.round(C.HALO_MAX * ctx.w);
  const wbs = [], bboxes = [], anchors = [];
  for (const d of dets) {
    const { win } = d;
    const E = buildErase(d.dist, d.G, d.prot, win.w, win.h, rH, rM, null);
    // guard band: ring pixels just above the cut, used only as resampling source so the ring stays continuous at the seam
    const gwin = { x: win.x, y: win.y - S4.GUARD_H, w: win.w, h: win.h + S4.GUARD_H };
    const E2 = new Uint8Array(gwin.w * gwin.h), guard = new Uint8Array(gwin.w * gwin.h), prot2 = new Uint8Array(gwin.w * gwin.h);
    E2.set(E, S4.GUARD_H * win.w);
    const gh = Math.round(S4.GUARD_HALF * ctx.w), rxl = Math.round(d.ax) - win.x;
    for (let y = 0; y < S4.GUARD_H; y++) for (let x = Math.max(0, rxl - gh); x <= Math.min(win.w - 1, rxl + gh); x++) guard[y * win.w + x] = 1;
    for (let y = 0; y < S4.GUARD_H; y++) for (let x = 0; x < win.w; x++) prot2[y * win.w + x] = 1;
    const wb = shrinkInto(ctx, out, gwin, E2, guard, prot2, d.ax, d.cut, f);
    wbs.push(wb); bboxes.push({ name: d.which, before: d.bbox }); anchors.push({ x: d.ax, y: d.cut });
  }
  return { ok: true, out, wb: wbs, det: dets, bboxes, anchors };
}

// ---------------------------------------------------------------- measure (used for post-check and by run.js)
function measure(ctx, slot) {
  if (slot === 2) { const d = detectSlot3(ctx); return d.ok ? { ok: true, boxes: [{ name: 'charm', bbox: d.bbox }] } : d; }
  const boxes = [];
  for (const which of ['gold', 'silver']) { const d = detectCharm4(ctx, which); if (!d.ok) return d; boxes.push({ name: which, bbox: d.bbox }); }
  return { ok: true, boxes };
}

// ---------------------------------------------------------------- main entry
async function capFlatSlot(outputBuf, templateBuf, opts) {
  try {
    const slot = opts && opts.slotIndex;
    const f = opts && opts.factor != null ? Number(opts.factor) : DEFAULT_FACTOR;
    if (slot !== 2 && slot !== 3) return skip(outputBuf, 'not a flat slot');
    if (!(f > 0.3 && f < 0.999)) return skip(outputBuf, 'factor out of range / no-op');
    let ctx;
    try { ctx = await decode(outputBuf); } catch (e) { return skip(outputBuf, 'undecodable or unsupported image (' + e.message + ')'); }
    if (ctx.w < 512 || ctx.h < 512 || Math.abs(ctx.w / ctx.h - 1) > 0.02) return skip(outputBuf, 'unexpected image size ' + ctx.w + 'x' + ctx.h);
    const b = estimateBg(ctx);
    ctx.bg = b.bg;
    if (b.flat < C.MIN_BG_FLAT || (b.bg[0] + b.bg[1] + b.bg[2]) / 3 < 200) return skip(outputBuf, 'background is not flat light (flat=' + b.flat.toFixed(2) + ')');
    const tg = await templateGate(ctx, templateBuf, slot);
    if (!tg.ok) return skip(outputBuf, tg.reason);

    const t0 = Date.now();
    const res = slot === 2 ? processSlot3(ctx, f) : processSlot4(ctx, f);
    if (!res.ok) return skip(outputBuf, res.reason);

    // post-check: re-measure the charm(s) on the result; ratios must match the factor
    const ctx2 = { data: res.out, w: ctx.w, h: ctx.h, ch: ctx.ch, bg: ctx.bg };
    const m2 = measure(ctx2, slot);
    if (!m2.ok) return skip(outputBuf, 'post-check: charm not re-detected (' + m2.reason + ')');
    const ratios = [];
    for (let i = 0; i < res.bboxes.length; i++) {
      const a = res.bboxes[i].before, c = m2.boxes[i].bbox;
      const sb = Math.max(a.x1 - a.x0 + 1, a.y1 - a.y0 + 1), sa = Math.max(c.x1 - c.x0 + 1, c.y1 - c.y0 + 1);
      const r = sa / sb; ratios.push(r);
      res.bboxes[i].after = c;
      if (Math.abs(r / f - 1) > C.RATIO_TOL) return skip(outputBuf, 'post-check: ratio ' + r.toFixed(3) + ' off target ' + f, { ratios });
    }
    const png = await sharp(res.out, { raw: { width: ctx.w, height: ctx.h, channels: ctx.ch } }).png({ compressionLevel: 6, adaptiveFiltering: false }).toBuffer();
    return {
      buf: png, changed: true, reason: 'ok',
      info: { slotIndex: slot, factor: f, bg: ctx.bg, charms: res.bboxes, anchors: res.anchors, ratios, touched: res.wb, ms: Date.now() - t0, template: tg }
    };
  } catch (e) {
    return skip(outputBuf, 'error: ' + (e && e.message));
  }
}

module.exports = { capFlatSlot, measure, decode, estimateBg, DEFAULT_FACTOR };
