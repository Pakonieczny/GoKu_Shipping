// TRACE QUALITY AND REPEATABILITY (charm-nest-ce-trace.js and the Image step's Place, charm-nest-ce-image.js; Paul, 28 Oct 2026: "I need the vectorized images to be very
// high quality" · "the Line Art option produces very poor results even when I import perfect black and white line images" · "The image vector Modal sometimes does
// not produce repeatable results"). No server, no network: the pictures are made here as raw pixels (node-canvas is not installed on the test machines).
//   (a) a photo-like picture, 5000 × 5000 (gradients, a face of soft shapes, an eye's fine lashes, strands of hair, a patch of tiny print, film grain), placed 30 mm on
//       a charm at Paul's settings (Levels 8, Detail 100, Smoothness 100, Despeckle 0) and at the defaults (4, 50, 50, 2), through the whole of Place as the page does
//       it (IM.placeSync: the same steps, here on one thread). The vector drawn back to pixels (1200 × 1200, 2 × 2 samples a pixel) is compared with the picture
//       itself (its luminance averaged in linear light, auto-levels): the mean absolute grey error (0..255), the same after a blur of 1.5 px (what the eye sees
//       at the laser's scale), and the IoU of every nested mask (that grey or darker) against the picture cut at the trace's own thresholds.
//   (b) a crisp line drawing, 600 × 450 (strokes 0.8 to 6 px wide, a zigzag, a square, a star, rings, a spiral, pairs of lines 1 to 2 px apart), LINE ART, placed
//       30 mm: IoU against the drawing itself at 8 × its pixels, the thin lines kept (the share of each one's middle inked), the gaps kept open, and how far the
//       vector's edge is from the drawing's (mean and 99th percentile, in the drawing's pixels).
//   (c) repeatability: the same picture and settings traced twice, by trace() and traceSync(), and through Place twice: the same paths, point for point.
//   node tests/charm-nest/ce-trace-quality.cjs        (QUALITY_BEFORE=<dir holding ce-trace.base.js and ce-image.base.js> measures the old code the same way;
//                                                      SHOTS=<dir> writes the pictures and the vectors drawn back as PNG)
'use strict';
const fs = require('fs'), path = require('path'), zlib = require('zlib'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const BEFORE = process.env.QUALITY_BEFORE || '';
const T = require(BEFORE ? path.join(BEFORE, 'ce-trace.base.js') : path.join(root, 'charm-nest-ce-trace.js'));
const IM = require(BEFORE ? path.join(BEFORE, 'ce-image.base.js') : path.join(root, 'charm-nest-ce-image.js'));
const SHOTS = process.env.SHOTS || '';
const results = [], failures = [];
const say = s => console.log('       ' + s);
async function step(name, fn) { try { await fn(); results.push(name); console.log('  ok   ' + name); } catch (e) { failures.push(name + ' :: ' + String(e && e.message || e).split('\n')[0]); console.log('  FAIL ' + name + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n         ')); } }
const f3 = v => (Math.round(v * 1000) / 1000).toFixed(3);
const now = () => Number(process.hrtime.bigint() / 1000n) / 1000;

/* ═══ the pictures ═══ */
/** A photo-like picture, S × S RGBA: a gradient and vignette, hair of fine wavy strands, a face (soft shading, eyes with iris and lashes, brows, a nose, a mouth),
 *  pores, a patch of tiny print, and film grain. Made the same every time (its own seeded noise). */
function photo(S) {
  const data = new Uint8ClampedArray(S * S * 4);
  let seed = 12345; const rnd = () => ((seed = (Math.imul(seed, 1103515245) + 12345) & 0x7fffffff) / 0x7fffffff);
  const sm = (a, b, x) => { const t = Math.max(0, Math.min(1, (x - a) / (b - a))); return t * t * (3 - 2 * t); };
  for (let y = 0; y < S; y++) {
    const v = y / S;
    for (let x = 0; x < S; x++) {
      const u = x / S;
      let g = 0.35 + 0.45 * (u * 0.6 + v * 0.4) - 0.25 * ((u - 0.5) ** 2 + (v - 0.5) ** 2);
      const hx = (u - 0.5) / 0.34, hy = (v - 0.42) / 0.36, hr = hx * hx + hy * hy;
      if (hr < 1) { const strand = 0.5 + 0.5 * Math.sin(u * 450 + 9 * Math.sin(v * 7) + 3 * Math.sin(u * 11)); g = 0.12 + 0.2 * strand * strand * strand + 0.1 * (1 - hr); }
      const fx = (u - 0.5) / 0.22, fy = (v - 0.5) / 0.3, fr = fx * fx + fy * fy;
      if (fr < 1) {
        const edge = sm(1, 0.85, fr);
        let f = 0.72 - 0.12 * fx - 0.1 * fr;
        for (const ex of [0.41, 0.59]) {
          const dx = (u - ex) / 0.045, dy = (v - 0.45) / 0.022, er = dx * dx + dy * dy;
          if (er < 1) { f = 0.92; const ir = ((u - ex) ** 2 + (v - 0.45) ** 2) / (0.016 * 0.016); if (ir < 1) f = 0.25 + 0.2 * ir + 0.08 * Math.sin(Math.atan2(v - 0.45, u - ex) * 24); if (ir < 0.25) f = 0.04; }
          const bd = Math.abs(v - (0.405 - 0.02 * Math.cos((u - ex) / 0.05 * 1.4)));
          if (Math.abs(u - ex) < 0.05 && bd < 0.006) f = Math.min(f, 0.25 + bd / 0.006 * 0.3);
          if (er > 1 && er < 1.6 && dy < 0 && Math.sin(Math.atan2(dy, dx) * 40) > 0.6) f = Math.min(f, 0.3);
        }
        f -= 0.12 * Math.exp(-(((u - 0.515) / 0.012) ** 2) - (((v - 0.53) / 0.06) ** 2));
        for (const nx of [0.485, 0.515]) if (((u - nx) / 0.012) ** 2 + ((v - 0.585) / 0.006) ** 2 < 1) f = 0.2;
        const my = 0.66 + 2.5 * (u - 0.5) * (u - 0.5);
        if (Math.abs(u - 0.5) < 0.07) { const d = Math.abs(v - my); if (d < 0.004) f = 0.15; else if (d < 0.018) f = Math.min(f, 0.45 + d * 10); }
        f += 0.03 * Math.sin(u * 3500) * Math.sin(v * 3500);
        g = g * (1 - edge) + f * edge;
      }
      if (u > 0.78 && v > 0.8) g = 0.85 - 0.6 * ((Math.sin(u * 1750) > 0.3) && (Math.sin(v * 600) > -0.2) ? 1 : 0);
      g += (rnd() + rnd() + rnd() - 1.5) * 0.04;
      const c = Math.max(0, Math.min(255, Math.round(g * 255))), k = (y * S + x) * 4;
      data[k] = Math.min(255, c + 12); data[k + 1] = c; data[k + 2] = Math.max(0, c - 10); data[k + 3] = 255;
    }
  }
  return { data, width: S, height: S };
}

/** A crisp line drawing (black ink on white), W × H at its own pixels, antialiased by SS × SS samples, and what it is made of, to measure against:
 *  { img, ink(x, y) (the drawing itself, any point), thin: [{ w, pts }] (the middles of the thin lines), gaps: [pts] (the middles of the gaps between pairs) } */
function drawing(W, H, SS) {
  const segs = [];   // [x0, y0, x1, y1, halfWidth]
  const poly = (pts, w, closed) => { for (let i = 0; i + 1 < pts.length; i++) segs.push([...pts[i], ...pts[i + 1], w / 2]); if (closed) segs.push([...pts[pts.length - 1], ...pts[0], w / 2]); };
  const k = W / 600;
  const P = (x, y) => [x * k, y * k];
  // a zigzag (60° corners), a square, a five-pointed star (36° tips)
  poly([P(30, 60), P(70, 20), P(110, 60), P(150, 20), P(190, 60), P(230, 20)], 4 * k);
  poly([P(260, 20), P(340, 20), P(340, 100), P(260, 100)], 3 * k, true);
  { const pts = []; for (let i = 0; i < 10; i++) { const a = -Math.PI / 2 + i * Math.PI / 5, r = i % 2 ? 18 : 45; pts.push(P(450 + r * Math.cos(a), 70 + r * Math.sin(a))); } poly(pts, 2.5 * k, true); }
  // rings and a spiral
  const ring = (cx, cy, r, w) => { const pts = []; for (let i = 0; i <= 96; i++) pts.push(P(cx + r * Math.cos(2 * Math.PI * i / 96), cy + r * Math.sin(2 * Math.PI * i / 96))); poly(pts, w * k); };
  ring(80, 200, 50, 6); ring(80, 200, 30, 1.5); ring(200, 200, 40, 2);
  { const pts = []; for (let i = 0; i <= 400; i++) { const a = i / 400 * 6 * Math.PI, r = 4 + a * 5; pts.push(P(340 + r * Math.cos(a), 200 + r * Math.sin(a))); } poly(pts, 1.6 * k); }
  // thin lines: 0.8, 1, 1.5, 2 px wide, long and slightly slanted
  const thin = [];
  [0.8, 1, 1.5, 2].forEach((w, i) => { const a = P(440, 150 + i * 22), b = P(580, 156 + i * 22); poly([a, b], w * k); thin.push({ w, a, b }); });
  // pairs of 1.5 px lines with gaps of 1, 1.5 and 2 px between them
  const gaps = [];
  [1, 1.5, 2].forEach((gap, i) => {
    const y = (300 + i * 40) * k, half = (1.5 * k) / 2, off = (gap * k) / 2 + half;
    poly([[40 * k, y - off], [260 * k, y - off]], 1.5 * k); poly([[40 * k, y + off], [260 * k, y + off]], 1.5 * k);
    gaps.push({ gap, a: [50 * k, y], b: [250 * k, y] });
  });
  // a hand-drawn-like curve and some hatching
  { const pts = []; for (let i = 0; i <= 200; i++) { const t = i / 200; pts.push(P(300 + 260 * t, 330 + 60 * Math.sin(t * 9) * (1 - t * 0.5))); } poly(pts, 3 * k); }
  for (let i = 0; i < 8; i++) poly([P(300 + i * 9, 410), P(330 + i * 9, 440)], 1.2 * k);
  // a grid of cells for the ink test: segments by row band
  const cell = 32, grid = new Map();
  for (const s of segs) {
    const x0 = Math.floor((Math.min(s[0], s[2]) - s[4] - 1) / cell), x1 = Math.floor((Math.max(s[0], s[2]) + s[4] + 1) / cell);
    const y0 = Math.floor((Math.min(s[1], s[3]) - s[4] - 1) / cell), y1 = Math.floor((Math.max(s[1], s[3]) + s[4] + 1) / cell);
    for (let gy = y0; gy <= y1; gy++) for (let gx = x0; gx <= x1; gx++) { const key = gy * 4096 + gx; if (!grid.has(key)) grid.set(key, []); grid.get(key).push(s); }
  }
  const ink = (x, y) => {
    const list = grid.get(Math.floor(y / cell) * 4096 + Math.floor(x / cell)); if (!list) return false;
    for (const s of list) {
      const dx = s[2] - s[0], dy = s[3] - s[1], L = dx * dx + dy * dy, t = L ? Math.max(0, Math.min(1, ((x - s[0]) * dx + (y - s[1]) * dy) / L)) : 0;
      const ex = x - s[0] - t * dx, ey = y - s[1] - t * dy; if (ex * ex + ey * ey <= s[4] * s[4]) return true;
    }
    return false;
  };
  const data = new Uint8ClampedArray(W * H * 4);
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
    let n = 0; for (let j = 0; j < SS; j++) for (let i = 0; i < SS; i++) if (ink(x + (i + 0.5) / SS, y + (j + 0.5) / SS)) n++;
    const v = Math.round(255 * (1 - n / (SS * SS))), p = (y * W + x) * 4; data[p] = data[p + 1] = data[p + 2] = v; data[p + 3] = 255;
  }
  return { img: { data, width: W, height: H }, ink, thin, gaps };
}

/* ═══ resampling, drawing back, measuring ═══ */
/** RGBA averaged down to W × H by area, in the numbers (as a browser's drawImage does) or in linear light. */
function down(src, W, H, linear) {
  const w = src.width, h = src.height, d = src.data, out = new Uint8ClampedArray(W * H * 4), sx = w / W, sy = h / H, acc = new Float64Array(W * 4);
  const LIN = new Float64Array(256); for (let i = 0; i < 256; i++) { const c = i / 255; LIN[i] = linear ? (c <= 0.04045 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4)) : c; }
  const enc = l => (linear ? (l <= 0.0031308 ? 12.92 * l : 1.055 * Math.pow(l, 1 / 2.4) - 0.055) : l);
  for (let Y = 0; Y < H; Y++) {
    acc.fill(0); const y0 = Y * sy, y1 = (Y + 1) * sy;
    for (let y = Math.floor(y0); y < Math.min(h, Math.ceil(y1)); y++) {
      const wy = Math.min(y1, y + 1) - Math.max(y0, y);
      for (let X = 0; X < W; X++) { const x0 = X * sx, x1 = (X + 1) * sx; for (let x = Math.floor(x0); x < Math.min(w, Math.ceil(x1)); x++) { const wgt = wy * (Math.min(x1, x + 1) - Math.max(x0, x)), q = (y * w + x) * 4; acc[X * 4] += LIN[d[q]] * wgt; acc[X * 4 + 1] += LIN[d[q + 1]] * wgt; acc[X * 4 + 2] += LIN[d[q + 2]] * wgt; acc[X * 4 + 3] += d[q + 3] * wgt; } }
    }
    for (let X = 0; X < W; X++) { const a = sx * sy, q = (Y * W + X) * 4; for (let c = 0; c < 3; c++) out[q + c] = Math.round(255 * enc(acc[X * 4 + c] / a)); out[q + 3] = Math.round(acc[X * 4 + 3] / a); }
  }
  return { data: out, width: W, height: H };
}
function flatten(sub, k) {
  const pts = []; let cur = null;
  for (const op of sub) {
    if (op[0] === 'm' || op[0] === 'l') { cur = [op[1][0] * k, op[1][1] * k]; pts.push(cur); }
    else if (op[0] === 'c') {
      const p1 = [op[1][0] * k, op[1][1] * k], p2 = [op[2][0] * k, op[2][1] * k], p3 = [op[3][0] * k, op[3][1] * k];
      const len = Math.hypot(p1[0] - cur[0], p1[1] - cur[1]) + Math.hypot(p2[0] - p1[0], p2[1] - p1[1]) + Math.hypot(p3[0] - p2[0], p3[1] - p2[1]), n = Math.max(4, Math.ceil(len / 0.3));
      for (let i = 1; i <= n; i++) { const t = i / n, u = 1 - t; pts.push([u * u * u * cur[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t * t * t * p3[0], u * u * u * cur[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t * t * t * p3[1]]); }
      cur = p3;
    }
  }
  return pts;
}
/** Subpaths (in units where the picture is `w` wide) filled into a W × H mask by the non-zero rule, sampled at the pixel centres. */
function raster(subpaths, k, W, H) {
  const cross = Array.from({ length: H }, () => []);
  for (const sub of subpaths) {
    const p = flatten(sub, k);
    for (let i = 0; i < p.length; i++) {
      const a = p[i], b = p[(i + 1) % p.length]; if (a[1] === b[1]) continue;
      const dir = b[1] > a[1] ? 1 : -1, ya = Math.min(a[1], b[1]), yb = Math.max(a[1], b[1]);
      for (let r = Math.max(0, Math.ceil(ya - 0.5)); r <= Math.min(H - 1, Math.ceil(yb - 0.5) - 1); r++) { const yc = r + 0.5; cross[r].push(a[0] + (b[0] - a[0]) * (yc - a[1]) / (b[1] - a[1]), dir); }
    }
  }
  const out = new Uint8Array(W * H);
  for (let r = 0; r < H; r++) {
    const c = cross[r], n = c.length / 2, ix = Array.from({ length: n }, (_, i) => i).sort((a, b) => c[2 * a] - c[2 * b]); let wind = 0;
    for (let i = 0; i < n - 1; i++) { wind += c[2 * ix[i] + 1]; if (wind) for (let x = Math.max(0, Math.ceil(c[2 * ix[i]] - 0.5)); x <= Math.min(W - 1, Math.ceil(c[2 * ix[i + 1]] - 0.5) - 1); x++) out[r * W + x] = 1; }
  }
  return out;
}
/** The vector drawn back as greys at E × E' (ss × ss samples a pixel): white, each band in its grey. */
function render(res, E, EH, ss) {
  const k = (E * ss) / res.width, W = E * ss, H = EH * ss, g = new Float32Array(W * H).fill(1);
  for (const L of res.layers) { const m = raster(L.subpaths, k, W, H); for (let i = 0; i < m.length; i++) if (m[i]) g[i] = Math.min(g[i], L.grey); }
  const out = new Float32Array(E * EH);
  for (let y = 0; y < EH; y++) for (let x = 0; x < E; x++) { let s = 0; for (let j = 0; j < ss; j++) for (let i = 0; i < ss; i++) s += g[(y * ss + j) * W + x * ss + i]; out[y * E + x] = s / (ss * ss); }
  return out;
}
/** The picture's own grey at E × E': luma of the linear-light average, auto-levels (0.5 % clipped at each end), as the trace's grey is made. */
function reference(img, E, EH) {
  const d = down(img, E, EH, true).data, n = E * EH, v = new Float32Array(n);
  for (let i = 0; i < n; i++) v[i] = (0.299 * d[i * 4] + 0.587 * d[i * 4 + 1] + 0.114 * d[i * 4 + 2]) / 255;
  const s = Float32Array.from(v).sort(), lo = s[Math.floor(n * 0.005)], hi = s[Math.min(n - 1, Math.floor(n * 0.995))];
  for (let i = 0; i < n; i++) v[i] = Math.max(0, Math.min(1, (v[i] - lo) / Math.max(0.02, hi - lo)));
  return v;
}
function blur(v, W, H, sigma) {
  const r = Math.ceil(sigma * 3), k = []; let t = 0; for (let i = -r; i <= r; i++) { k.push(Math.exp(-i * i / (2 * sigma * sigma))); t += k[k.length - 1]; } for (let i = 0; i < k.length; i++) k[i] /= t;
  const a = new Float32Array(W * H), b = new Float32Array(W * H);
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) { let s = 0; for (let i = -r; i <= r; i++) s += v[y * W + Math.max(0, Math.min(W - 1, x + i))] * k[i + r]; a[y * W + x] = s; }
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) { let s = 0; for (let i = -r; i <= r; i++) s += a[Math.max(0, Math.min(H - 1, y + i)) * W + x] * k[i + r]; b[y * W + x] = s; }
  return b;
}
const mae = (a, b) => { let s = 0; for (let i = 0; i < a.length; i++) s += Math.abs(a[i] - b[i]); return 255 * s / a.length; };
function iou(a, b) { let both = 0, either = 0; for (let i = 0; i < a.length; i++) { if (a[i] && b[i]) both++; if (a[i] || b[i]) either++; } return either ? both / either : 1; }
/** The measures of a greys trace against the picture: { mae, maeBlur, iou: [per nested mask] }. */
function photoQuality(res, ref, E, EH) {
  const g = render(res, E, EH, 2), m = mae(g, ref), mb = mae(blur(g, E, EH, 1.5), blur(ref, E, EH, 1.5)), N = res.levels, ious = [];
  for (let k = 0; k < N; k++) {
    const t = res.thresholds[k], gk = (k + 0.5) / N;
    const a = new Uint8Array(g.length), b = new Uint8Array(g.length);
    for (let i = 0; i < g.length; i++) { a[i] = g[i] < gk ? 1 : 0; b[i] = ref[i] <= t ? 1 : 0; }
    ious.push(iou(a, b));
  }
  return { mae: m, maeBlur: mb, iou: ious, grey: g };
}
/** Euclidean-ish (chamfer 3-4) distance, in pixels, from every pixel to the nearest edge pixel of a mask. */
function edgeDist(mask, W, H) {
  const INF = 1e9, d = new Float32Array(W * H).fill(INF);
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) { const i = y * W + x, m = mask[i]; if ((x > 0 && mask[i - 1] !== m) || (x < W - 1 && mask[i + 1] !== m) || (y > 0 && mask[i - W] !== m) || (y < H - 1 && mask[i + W] !== m)) d[i] = 0; }
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) { const i = y * W + x; let v = d[i]; if (x > 0) v = Math.min(v, d[i - 1] + 3); if (y > 0) { v = Math.min(v, d[i - W] + 3); if (x > 0) v = Math.min(v, d[i - W - 1] + 4); if (x < W - 1) v = Math.min(v, d[i - W + 1] + 4); } d[i] = v; }
  for (let y = H - 1; y >= 0; y--) for (let x = W - 1; x >= 0; x--) { const i = y * W + x; let v = d[i]; if (x < W - 1) v = Math.min(v, d[i + 1] + 3); if (y < H - 1) { v = Math.min(v, d[i + W] + 3); if (x < W - 1) v = Math.min(v, d[i + W + 1] + 4); if (x > 0) v = Math.min(v, d[i + W - 1] + 4); } d[i] = v; }
  for (let i = 0; i < d.length; i++) d[i] /= 3;
  return d;
}
/** The measures of a line-art trace against the drawing, at Z × its pixels: IoU, thin lines kept, gaps open, edge distance (mean, 99th percentile, drawing px). */
function lineQuality(res, art, Z) {
  const W = res.width * Z, H = res.height * Z, layer = res.layers.find(L => L.level === 0), got = layer ? raster(layer.subpaths, Z, W, H) : new Uint8Array(W * H), truth = new Uint8Array(W * H);
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) truth[y * W + x] = art.ink((x + 0.5) / Z, (y + 0.5) / Z) ? 1 : 0;
  const at = (p) => got[Math.min(H - 1, Math.max(0, Math.floor(p[1] * Z))) * W + Math.min(W - 1, Math.max(0, Math.floor(p[0] * Z)))];
  const along = (a, b, f) => { let n = 0, hit = 0; for (let i = 0; i <= 200; i++) { const t = 0.05 + 0.9 * i / 200; n++; if (f(at([a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]))) hit++; } return hit / n; };
  const thin = art.thin.map(l => ({ w: l.w, kept: along(l.a, l.b, v => v === 1) }));
  const gaps = art.gaps.map(g => ({ gap: g.gap, open: along(g.a, g.b, v => v === 0) }));
  // edge distance: each edge pixel of either, to the other's nearest edge
  const dT = edgeDist(truth, W, H), dG = edgeDist(got, W, H), ds = [];
  for (let y = 1; y < H - 1; y++) for (let x = 1; x < W - 1; x++) { const i = y * W + x; if (dG[i] === 0) ds.push(dT[i]); if (dT[i] === 0) ds.push(dG[i]); }
  ds.sort((a, b) => a - b);
  const mean = ds.reduce((a, b) => a + b, 0) / Math.max(1, ds.length) / Z, p99 = (ds[Math.floor(ds.length * 0.99)] || 0) / Z;
  return { iou: iou(got, truth), thin, gaps, edgeMean: mean, edgeP99: p99, got, truth, W, H };
}
/** A grey picture (0..1 or 0..255) as a PNG (for SHOTS). */
function png(file, v, W, H, scale) {
  const crcT = new Int32Array(256).map((_, n) => { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; return c; });
  const crc = b => { let c = -1; for (const x of b) c = crcT[(c ^ x) & 255] ^ (c >>> 8); return (c ^ -1) >>> 0; };
  const chunk = (t, d) => { const l = Buffer.alloc(4); l.writeUInt32BE(d.length); const td = Buffer.concat([Buffer.from(t), d]); const c = Buffer.alloc(4); c.writeUInt32BE(crc(td)); return Buffer.concat([l, td, c]); };
  const raw = Buffer.alloc((W + 1) * H); for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) raw[y * (W + 1) + 1 + x] = Math.max(0, Math.min(255, Math.round(v[y * W + x] * (scale || 255))));
  const ih = Buffer.alloc(13); ih.writeUInt32BE(W, 0); ih.writeUInt32BE(H, 4); ih[8] = 8; ih[9] = 0;
  fs.writeFileSync(file, Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ih), chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]));
}

/* ═══ Place, as the page does it ═══ */
const SRC_MAX = 2400;
/** The picture as the page keeps it (decoded at most SRC_MAX px a side: the browser's drawImage, here an average of the numbers). */
const kept = img => { const k = Math.min(1, SRC_MAX / Math.max(img.width, img.height)); return k < 1 ? down(img, Math.round(img.width * k), Math.round(img.height * k), false) : img; };
/** Place: the new code's own (IM.placeSync), or the old code's, step for step (charm-nest-ce-image.js before 28 Oct 2026 TRACEHQ: the picture drawn at 20 px a mm, at
 *  most 1400, prepared, traced with the preview's thresholds, and traced again at 0.72 × while it is more than a piece's record holds). */
function place(src, settings, longMm) {
  const t0 = now();
  if (typeof IM.placeSync === 'function') { const r = IM.placeSync(src, settings, longMm); r.placeMs = now() - t0; return r; }
  const st = IM.norm(settings), [kw, kh] = [src.width, src.height];
  // the preview's thresholds: the grey at 560 px drawn down to 280, posterised
  const fit = (w, h, m) => { const k = Math.min(1, m / Math.max(w, h)); return [Math.max(1, Math.round(w * k)), Math.max(1, Math.round(h * k))]; };
  const [sw, sh] = fit(kw, kh, 560), show = down(src, sw, sh, false), g560 = IM.greyImage(show.data, sw, sh, st), [pw, ph] = fit(sw, sh, 280), g280 = down(g560, pw, ph, false);
  const opts0 = IM.traceOpts(st, Math.max(pw, ph) / longMm), pz = T.posterise(g280, Object.assign({ maxSide: 280 }, opts0)), thr = pz.thresholds;
  let px = Math.min(Math.max(16, Math.min(Math.round(longMm * 20), 1400)), Math.round(Math.max(kw, kh))), res, size, tries = 0;
  for (;;) {
    const [w, h] = fit(kw, kh, px), inp = down(src, w, h, false), grey = IM.greyImage(inp.data, w, h, st);
    res = T.traceSync(grey, Object.assign(IM.traceOpts(st, Math.max(w, h) / longMm), { thresholds: thr }));
    size = IM.sizeOf(res.layers);
    if (IM.fits(size) || ++tries > 4 || px <= 64) break;
    px = Math.max(64, Math.round(px * 0.72));
  }
  return { res, size, fits: IM.fits(size), px: res.width, placeMs: now() - t0, tries };
}

module.exports = { photo, drawing, down, render, reference, photoQuality, lineQuality, place, kept, png, raster };
if (require.main !== module) return;

(async () => {
  const E = 1200, MM = 30, PHOTO_SIDE = +(process.env.PHOTO_SIDE || 5000), tag = BEFORE ? 'before' : 'after';
  let t = now(); const big = photo(PHOTO_SIDE); say(`photo ${PHOTO_SIDE} × ${PHOTO_SIDE} made in ${(now() - t).toFixed(0)} ms`);
  t = now(); const pic = kept(big), ref = reference(pic, E, E); say(`kept at ${pic.width} px (as the page decodes it) in ${(now() - t).toFixed(0)} ms`);
  if (SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); png(path.join(SHOTS, 'photo-source.png'), ref, E, E); }
  const Q = {};
  for (const [name, s] of [['max', { levels: 8, detail: 1, smooth: 1, despeckle: 0 }], ['default', { levels: 4, detail: 0.5, smooth: 0.5, despeckle: 2 }]]) {
    await step(`(a) the photo at ${name} settings (${JSON.stringify(s)}), placed ${MM} mm: tone and shape against the picture`, () => {
      const p = place(pic, s, MM), q = photoQuality(p.res, ref, E, E); Q[name] = { p, q };
      say(`${tag}: traced at ${p.res.work ? p.res.work.width + ' × ' + p.res.work.height : p.px + ' px'}${p.tries ? ` (${p.tries} × made smaller to fit)` : ''}${p.res.work && p.res.work.simplified ? ` (simplified ${p.res.work.simplified} steps to fit)` : ''}, ${p.res.nodes} nodes, ${p.size.ops} ops, ${(p.size.bytes / 1000).toFixed(0)} KB${p.fits ? '' : ' (TOO BIG for a piece)'}, Place ${p.placeMs.toFixed(0)} ms (trace ${p.res.ms.work.toFixed(0)})`);
      say(`  grey error ${q.mae.toFixed(2)} / 255 · blurred 1.5 px ${q.maeBlur.toFixed(2)} · IoU per nested mask ${q.iou.map(f3).join(' ')} · thresholds ${p.res.thresholds.join(' ')}`);
      if (SHOTS) png(path.join(SHOTS, `photo-${name}-${tag}.png`), q.grey, E, E);
      if (!BEFORE) {
        assert.ok(p.fits, 'what a piece can hold');
        assert.ok(q.maeBlur < (name === 'max' ? 9 : 12), 'the blurred grey error ' + q.maeBlur);
        for (const v of q.iou) assert.ok(v > 0.85, 'IoU per mask ' + v);
      }
    });
  }
  const art = drawing(600, 450, 8);
  if (SHOTS) png(path.join(SHOTS, 'line-source.png'), (() => { const v = new Float32Array(600 * 450); for (let i = 0; i < v.length; i++) v[i] = art.img.data[i * 4] / 255; return v; })(), 600, 450);
  await step(`(b) a crisp line drawing (600 × 450), LINE ART, placed ${MM} mm: the outline against the drawing at 8 ×`, () => {
    const p = place(art.img, { mode: 'line', detail: 0.5, smooth: 0.5, despeckle: 2 }, MM), q = lineQuality(p.res, art, 8);
    say(`${tag}: traced at ${p.res.work ? p.res.work.width + ' × ' + p.res.work.height : p.px + ' px'}, ${p.res.nodes} nodes, Place ${p.placeMs.toFixed(0)} ms`);
    say(`  IoU ${f3(q.iou)} · thin lines kept ${q.thin.map(l => l.w + ' px ' + (100 * l.kept).toFixed(0) + '%').join(', ')} · gaps open ${q.gaps.map(g => g.gap + ' px ' + (100 * g.open).toFixed(0) + '%').join(', ')} · edge off by ${q.edgeMean.toFixed(3)} px mean, ${q.edgeP99.toFixed(3)} px p99`);
    if (SHOTS) { const v = new Float32Array(q.W * q.H); for (let i = 0; i < v.length; i++) v[i] = q.got[i] ? 0 : 1; png(path.join(SHOTS, `line-${tag}.png`), v, q.W, q.H); }
    if (!BEFORE) {
      assert.ok(q.iou > 0.95, 'IoU ' + q.iou);
      for (const l of q.thin) assert.ok(l.kept > 0.97, l.w + ' px line kept ' + l.kept);
      for (const g of q.gaps) assert.ok(g.open > 0.97, g.gap + ' px gap open ' + g.open);
      assert.ok(q.edgeMean < 0.12 && q.edgeP99 < 0.6, 'edge ' + q.edgeMean + ' / ' + q.edgeP99);
    }
  });
  console.log(`\n${results.length} passed, ${failures.length} failed`);
  if (failures.length) { console.log(failures.map(f => '  - ' + f).join('\n')); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
