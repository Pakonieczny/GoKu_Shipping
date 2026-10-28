/* charm-nest-ce-trace.js — IMAGE TRACE for the Charm Editor: a picture made into smooth filled vector paths, greyscale with black as the base (TRACE, Paul,
 * 28 Oct 2026: "All images must be converted to a vector format while keeping the quality of the image and the colour should be a greyscale using the black as
 * base"; items 12 and 13; the lead's D2: the image is made grey, posterised into N levels (default 4, 1–8; 1 = a plain black / white threshold), each level traced
 * to smooth filled paths, black the darkest level, white no ink, each level keeping its grey so the laser operator gives each grey its own power).
 * The import UI (charm-nest-ce-image.js) calls it; this file draws nothing and saves nothing. It loads in the page (window.CharmNestTrace), in its own Worker made
 * from a Blob URL of this same file's code (no second served file), and in node (module.exports, for the tests).
 *
 *   CharmNestTrace.trace(src, opts) -> Promise<result>     src: ImageData | { data, width, height } (RGBA, RGB or one grey channel, 0..255)
 *     opts.levels 4          1..8 inked greys; white is never inked
 *     opts.detail 0.5        0..1, how closely the curves keep to the pixels: Potrace's opttolerance 0.4 · 0.2 · 0.1 at 0 · 0.5 · 1; under 0.25 a median denoise
 *     opts.smooth 0.5        0..1, corners or curves: Potrace's alphamax 0 · 1.0 · 1.333 at 0 · 0.5 · 1 (0 = a polygon, 1 = no corner anywhere)
 *     opts.despeckle 2       specks of up to this many pixels AT 20 px/mm (0.0025 mm² each, so 2 = 0.005 mm², Potrace's turdsize 2) are merged into what
 *                            surrounds them; opts.despeckleMm2 says it in mm² on the charm and wins when given (the lead, 28 Oct 2026: "despeckle measured in mm²")
 *     opts.pxPerMm 20        the source's pixels per mm as placed on the charm. The trace works at workPxPerMm (20 px/mm, ~500 ppi: a fibre laser's hatch pitch
 *                            is 0.02–0.05 mm, so finer buys nothing): a small source is enlarged (bicubic), a big one averaged down, never past maxSide 2048 a side
 *     opts.invert, contrast, brightness, gamma, autoLevels, sharpen, denoise, adaptive   the grey() steps below, under the same names
 *     opts.thresholds        'otsu' (default: multi-level Otsu, so every level holds real area) | 'even' | [t0 < t1 < …] N numbers 0..1 (from posterise())
 *     opts.stack false       true: each layer is its whole nested mask (Inkscape's "stack scans"; the lighter ones must then be drawn underneath)
 *     opts.onProgress(p)     0..1, on the page's thread;   opts.signal   an AbortSignal
 *   result { width, height, layers:[{ grey, level, subpaths, nodes, areaPx }], levels, thresholds, greys, suggestLevels, content, midtones, nodes, scale, ms, worker }
 *     subpaths are the editor's ops ["m",[x,y]] ["l",[x,y]] ["c",p1,p2,p3] ["h"] in SOURCE pixels, y down, pixel corners on whole numbers; outer borders run
 *     clockwise on screen and holes the other way, so the non-zero and the even-odd rules fill them alike. layers are darkest first; level k of N has
 *     grey = k / N (0, .25, .5, .75 for 4): fixed, so a grey means the same laser layer from job to job (the lead's research, 28 Oct 2026, §4.4); a level
 *     with no ink is left out. suggestLevels: 1 for line art, 4 for an illustration, 5 for a photo (content 'line' | 'illustration' | 'photo').
 *   CharmNestTrace.cancel()            every trace running or waiting rejects with an Error named 'AbortError' (.cancelled = true); the Worker is stopped
 *   CharmNestTrace.grey(src, opts) -> ImageData                 the greyscale-with-black-base preview (opts.maxSide for a smaller one)
 *   CharmNestTrace.posterise(src, opts) -> { image, width, height, thresholds, greys, suggestLevels, content, midtones }   the levels as they will be traced,
 *                                      small and quick (opts.maxSide 512, a few ms) for a live preview under a slider; pass its thresholds to trace()
 *   CharmNestTrace.suggest(src) -> { levels, content, midtones }
 *   CharmNestTrace.traceSync(src, opts) -> result                the same trace at once on this thread (node and the tests)
 *
 * THE GREY (what every laser prep guide does, in this order): luminance 0.299 R + 0.587 G + 0.114 B, a transparent part on white (no ink); auto-levels (0.5 %
 * clipped at each end: the darkest becomes black, the base); gamma (a number, or 'auto' to centre the histogram); brightness and contrast (-1..1); an unsharp
 * mask of radius 0.05 mm ('auto': amount 1.5 on a photo only, softened under 1 % grey; on flat art it would ring every edge into a false band); a 3 × 3 median
 * ('auto': on a photo, or at detail under 0.25); invert. adaptive: true divides by the local background first (a window an eighth of the image), for a phone
 * photo of handwriting under uneven light (LightBurn's "sketch" trace). Then the levels: multi-level Otsu on the histogram (or the caller's thresholds).
 * suggestLevels comes from the mid-tone share (20–80 % grey) after auto-levels (the studio's test, J 14835): under 15 % line art, else an illustration when
 * six of 64 tones hold 60 % of the picture, else a photo.
 *
 * BANDS THAT SHARE THEIR BORDERS (the laser's biggest pitfall: levels traced on their own leave hairline gaps, never engraved, or overlaps, burnt twice). The
 * level masks are NESTED: M_k = every pixel as dark as level k or darker, M_0 ⊂ M_1 ⊂ … ⊂ M_(N-1). Each M_k is traced once; band k is M_k's curves plus
 * M_(k-1)'s curves REVERSED, i.e. M_k minus M_(k-1). So the border between two bands is the very same curve in both: nothing between them, nothing under both,
 * each pixel burnt once at its own grey's power. A border that is the same in both masks (black straight onto white) cancels out of the band between. Despeckle
 * works on the levels, so the masks stay nested: a speck (up to the despeckle area) and a SEAM (a band under 0.075 mm wide between a darker and a lighter
 * level: an antialiased edge, which would engrave as a hairline ring round every shape) join the level they border most. Measured (circles in four greys,
 * supersampled 4 × 4): the bands overlap on 0.007 % of the ink, in slivers under a fifth of a pixel, and add up to M_(N-1) within 0.02 %.
 *
 * THE TRACE is written here from Peter Selinger's paper "Potrace: a polygon-based tracing algorithm" (2003; no code taken from potrace, which is GPL): each
 * border followed along the pixel edges (dark pixels kept joined at a corner, the same in every mask); the straight runs (a run is straight when no four
 * directions occur and a line passes within one pixel of every point; found with a cone of allowed directions per start); the optimal polygon (fewest edges,
 * then the least penalty |v_j - v_i| × rms distance, by layers between a greedy forward and a greedy backward bound); each vertex moved, within its pixel, to
 * best fit the two neighbouring edges' least-squares lines; at each vertex alpha = 4/3 (1 - h/d) from its distance d to the chord of the neighbouring midpoints
 * (h the reach of the pixel square across that chord): a corner at alpha >= alphamax, else a Bézier with alpha clamped to .55..1, then moved half way to the
 * alpha that keeps the pixels' own area (FULLNESS, below: not in the paper); then runs of curves turning the same way by less than 179° are joined into one
 * Bézier of the same area wherever it stays within opttolerance of the old ones (fewest curves, then the least error). Collinear midpoints between two corners
 * are dropped, and no point (control points too) lies outside the picture.
 *
 * SPEED (Paul, 28 Oct 2026: "no waiting"): the work runs in the Worker, posting progress; the page only unpacks the result, in slices of under 8 ms. Without a
 * Worker it runs on the page in slices of under 8 ms (each step of the work is a generator that yields every ~32 k pixels or so many path points). cancel()
 * stops the Worker at once. tests/charm-nest/ce-trace.cjs measures it (28 Oct 2026, this machine under load): a 1-megapixel photo at 4 levels in ~0.3–0.5 s in
 * node, ~0.6 s through the browser's Worker with no long task on the page; IoU of the result drawn back against the masks it was traced from 0.97–0.99 on
 * that photo, over 0.99 on drawn art. */
(function (root, factory) {
  const api = factory(root, false);
  if (typeof module === 'object' && module.exports) module.exports = api; else if (api) root.CharmNestTrace = api;
})(typeof self !== 'undefined' ? self : this, function charmNestTraceFactory(root, inWorker) {
  'use strict';
  const now = () => (typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now());
  const clamp = (v, a, b) => (v < a ? a : v > b ? b : v);
  const num = (v, d) => (typeof v === 'number' && Number.isFinite(v) ? v : d);
  const ROW_PX = 32768;                 // pixels between two yields of a pixel pass
  const PATH_PTS = 12000;               // path points (and scanned pixels / 8) between two yields of the trace
  const SLICE_MS = 8;                   // the page's own slices, when it does the work itself
  const SEAM_MM = 0.075;                // a band between a darker and a lighter level thinner than this on average is an edge's antialiasing (1.5 px at 20 px/mm)
  const DEFAULTS = Object.freeze({ levels: 4, detail: 0.5, smooth: 0.5, despeckle: 2, pxPerMm: 20, workPxPerMm: 20, maxSide: 2048, invert: false,
    contrast: 0, brightness: 0, gamma: 1, autoLevels: true, sharpen: 'auto', denoise: 'auto', adaptive: false, thresholds: 'otsu', stack: false });

  function options(o) {
    o = Object.assign({}, DEFAULTS, o || {});
    o.levels = clamp(Math.round(num(o.levels, 4)), 1, 8);
    o.detail = clamp(num(o.detail, 0.5), 0, 1);
    o.smooth = clamp(num(o.smooth, 0.5), 0, 1);
    o.pxPerMm = num(o.pxPerMm, 20) > 0 ? +o.pxPerMm : 20;
    o.workPxPerMm = num(o.workPxPerMm, 20) > 0 ? +o.workPxPerMm : 20;
    o.maxSide = Math.max(16, Math.round(num(o.maxSide, 2048)));
    // the speck size in mm² on the charm: despeckleMm2 when given, else despeckle pixels at 20 px/mm
    o.speckMm2 = num(o.despeckleMm2, null) != null ? Math.max(0, o.despeckleMm2) : Math.max(0, num(o.despeckle, 2)) / 400;
    // Potrace's two knobs (its defaults at 0.5 each): alphamax 0..1..1.333 and opttolerance 0.4..0.2..0.1
    o.alphamax = o.smooth <= 0.5 ? o.smooth * 2 : 1 + (o.smooth - 0.5) * (2 / 3);
    o.opttol = 0.2 * Math.pow(2, 2 * (0.5 - o.detail));
    return o;
  }

  /* ═══ 1 · the grey ═══ */
  function channelsOf(src) {
    const w = src && src.width | 0, h = src && src.height | 0, len = src && src.data ? src.data.length : 0;
    if (!(w > 0 && h > 0)) throw new Error('CharmNestTrace: the image has no size');
    if (len === w * h * 4) return 4; if (len === w * h) return 1; if (len === w * h * 3) return 3;
    throw new Error('CharmNestTrace: ' + len + ' values for a ' + w + ' × ' + h + ' image');
  }
  /** Luminance 0..1, a transparent part on white (no ink). */
  function* luminanceG(data, w, h, ch, p0, p1) {
    const out = new Float32Array(w * h);
    yield* rows(h, w, (y0, y1) => {
      if (ch === 1) { for (let i = y0 * w, e = y1 * w; i < e; i++) out[i] = data[i] / 255; return; }
      for (let i = y0 * w, e = y1 * w, j = i * ch; i < e; i++, j += ch) {
        let v = (0.299 * data[j] + 0.587 * data[j + 1] + 0.114 * data[j + 2]) / 255;
        if (ch === 4) { const a = data[j + 3] / 255; v = v * a + 1 - a; }
        out[i] = v;
      }
    }, p0, p1);
    return out;
  }
  /** Separable resampling weights: an exact area average going down, Catmull-Rom going up. */
  function weights(n, N) {
    const s = N / n, off = new Int32Array(N + 1), idx = [], wt = [];
    for (let i = 0; i < N; i++) {
      off[i] = idx.length;
      if (s < 1) {
        const a = i / s, b = (i + 1) / s, s0 = Math.floor(a), s1 = Math.min(n, Math.ceil(b));
        for (let k = s0; k < s1; k++) { idx.push(k); wt.push((Math.min(b, k + 1) - Math.max(a, k)) * s); }
      } else {
        const c = (i + 0.5) / s - 0.5, k0 = Math.floor(c) - 1;
        for (let t = 0; t < 4; t++) {
          const x = Math.abs(c - (k0 + t));
          const w = x <= 1 ? 1.5 * x * x * x - 2.5 * x * x + 1 : x < 2 ? -0.5 * x * x * x + 2.5 * x * x - 4 * x + 2 : 0;
          idx.push(clamp(k0 + t, 0, n - 1)); wt.push(w);
        }
      }
    }
    off[N] = idx.length;
    return { off, idx: Int32Array.from(idx), wt: Float32Array.from(wt) };
  }
  function* resampleG(v, w, h, W, H, p0, p1) {
    if (W === w && H === h) return v;
    const kx = weights(w, W), ky = weights(h, H), tmp = new Float32Array(W * h), out = new Float32Array(W * H);
    const pm = (p0 + p1) / 2;
    yield* rows(h, w, (y0, y1) => {
      for (let y = y0; y < y1; y++) {
        const r = y * w, o = y * W;
        for (let x = 0; x < W; x++) { let s = 0; for (let t = kx.off[x], e = kx.off[x + 1]; t < e; t++) s += v[r + kx.idx[t]] * kx.wt[t]; tmp[o + x] = s; }
      }
    }, p0, pm);
    yield* rows(H, W, (y0, y1) => {
      for (let y = y0; y < y1; y++) {
        const o = y * W;
        for (let t = ky.off[y], e = ky.off[y + 1]; t < e; t++) {
          const r = ky.idx[t] * W, k = ky.wt[t];
          for (let x = 0; x < W; x++) out[o + x] += tmp[r + x] * k;
        }
        for (let x = 0; x < W; x++) { const q = out[o + x]; out[o + x] = q < 0 ? 0 : q > 1 ? 1 : q; }
      }
    }, pm, p1);
    return out;
  }
  /** Pixel passes in chunks of rows, a yield between chunks (the page's own slices stay short). */
  function* rows(H, W, fn, p0, p1) {
    const step = Math.max(1, Math.floor(ROW_PX / Math.max(1, W)));
    for (let y = 0; y < H; y += step) { fn(y, Math.min(H, y + step)); yield p0 + (p1 - p0) * Math.min(1, (y + step) / H); }
  }
  function histOf(v) { const hist = new Uint32Array(1024); for (let i = 0; i < v.length; i++) hist[(v[i] * 1023 + 0.5) | 0]++; return hist; }
  function percentile(hist, n, q) {
    const want = q * n; let acc = 0;
    for (let b = 0; b < 1024; b++) { acc += hist[b]; if (acc >= want) return b / 1023; }
    return 1;
  }
  function* blurG(v, W, H, sigma, p0, p1) {
    const r = Math.max(1, Math.ceil(sigma * 3)), k = new Float32Array(2 * r + 1);
    let tot = 0; for (let i = -r; i <= r; i++) tot += (k[i + r] = Math.exp(-i * i / (2 * sigma * sigma)));
    for (let i = 0; i < k.length; i++) k[i] /= tot;
    const tmp = new Float32Array(W * H), out = new Float32Array(W * H), pm = (p0 + p1) / 2;
    yield* rows(H, W, (y0, y1) => {
      for (let y = y0; y < y1; y++) {
        const o = y * W;
        for (let x = 0; x < W; x++) { let s = 0; for (let t = -r; t <= r; t++) { const xx = x + t; s += v[o + (xx < 0 ? 0 : xx >= W ? W - 1 : xx)] * k[t + r]; } tmp[o + x] = s; }
      }
    }, p0, pm);
    yield* rows(H, W, (y0, y1) => {
      for (let y = y0; y < y1; y++) {
        const o = y * W;
        for (let t = -r; t <= r; t++) { const yy = y + t, row = (yy < 0 ? 0 : yy >= H ? H - 1 : yy) * W, kk = k[t + r]; for (let x = 0; x < W; x++) out[o + x] += tmp[row + x] * kk; }
      }
    }, pm, p1);
    return out;
  }
  /** 3 × 3 median: the 19-exchange network for nine values (Paeth's), edges kept as they are. */
  function* medianG(v, W, H, p0, p1) {
    const out = Float32Array.from(v);
    yield* rows(H, W, (y0, y1) => {
      for (let y = Math.max(1, y0); y < Math.min(H - 1, y1); y++) {
        for (let x = 1, i = y * W + 1; x < W - 1; x++, i++) {
          let a = v[i - W - 1], b = v[i - W], c = v[i - W + 1], d = v[i - 1], e = v[i], f = v[i + 1], g = v[i + W - 1], h = v[i + W], j = v[i + W + 1], t;
          if (b > c) { t = b; b = c; c = t; } if (e > f) { t = e; e = f; f = t; } if (h > j) { t = h; h = j; j = t; }
          if (a > b) { t = a; a = b; b = t; } if (d > e) { t = d; d = e; e = t; } if (g > h) { t = g; g = h; h = t; }
          if (b > c) { t = b; b = c; c = t; } if (e > f) { t = e; e = f; f = t; } if (h > j) { t = h; h = j; j = t; }
          if (a > d) { d = a; } if (f > j) { f = j; } if (e > h) { t = e; e = h; h = t; }
          if (d > g) { g = d; } if (b > e) { e = b; } if (c > f) { t = c; c = f; f = t; }
          if (e > h) { e = h; } if (c > e) { t = c; c = e; e = t; } if (g > e) { e = g; } if (e > c) { e = c; }
          out[i] = e;
        }
      }
    }, p0, p1);
    return out;
  }
  /** The local background (a box mean over `win` pixels), from an integral image. */
  function boxMean(v, W, H, win) {
    const S = new Float64Array((W + 1) * (H + 1)), r = win >> 1, out = new Float32Array(W * H);
    for (let y = 0; y < H; y++) { let row = 0; for (let x = 0; x < W; x++) { row += v[y * W + x]; S[(y + 1) * (W + 1) + x + 1] = S[y * (W + 1) + x + 1] + row; } }
    for (let y = 0; y < H; y++) {
      const a = Math.max(0, y - r), b = Math.min(H, y + r + 1);
      for (let x = 0; x < W; x++) {
        const c = Math.max(0, x - r), d = Math.min(W, x + r + 1);
        out[y * W + x] = (S[b * (W + 1) + d] - S[a * (W + 1) + d] - S[b * (W + 1) + c] + S[a * (W + 1) + c]) / ((b - a) * (d - c));
      }
    }
    return out;
  }
  /** What kind of picture: the mid-tone share after auto-levels (the studio's test, J 14835) and how much of it sits in a few tones. */
  function contentOf(v) {
    const n = v.length, hist = new Uint32Array(64); let mid = 0;
    for (let i = 0; i < n; i++) { const q = v[i]; hist[(q * 63 + 0.5) | 0]++; if (q > 0.2 && q < 0.8) mid++; }
    const midtones = mid / n, top = Array.from(hist).sort((a, b) => b - a), top6 = (top[0] + top[1] + top[2] + top[3] + top[4] + top[5]) / n;
    const content = midtones < 0.15 ? 'line' : top6 >= 0.6 ? 'illustration' : 'photo';
    return { content, midtones: Math.round(midtones * 1000) / 1000, suggestLevels: content === 'line' ? 1 : content === 'illustration' ? 4 : 5 };
  }
  /** grey() in place on a working-size grey image: adaptive → auto-levels → gamma → brightness / contrast → (content) → unsharp → median → invert. */
  function* prepG(v, W, H, o, ppm, info, p0, p1) {
    const P = f => p0 + (p1 - p0) * f;
    if (o.adaptive) {
      const bg = boxMean(v, W, H, Math.max(9, Math.round(Math.max(W, H) / 8)) | 1);
      for (let i = 0; i < v.length; i++) { const q = v[i] / Math.max(bg[i], 0.05); v[i] = q > 1 ? 1 : q; }
      yield P(0.1);
    }
    if (o.autoLevels !== false) {
      const hist = histOf(v), lo = percentile(hist, v.length, 0.005), hi = percentile(hist, v.length, 0.995);
      yield P(0.15);
      if (hi - lo > 0.02) { const s = 1 / (hi - lo); for (let i = 0; i < v.length; i++) { const q = (v[i] - lo) * s; v[i] = q < 0 ? 0 : q > 1 ? 1 : q; } }
      yield P(0.2);
    }
    let g = o.gamma;
    if (g === 'auto') { const med = percentile(histOf(v), v.length, 0.5); g = med > 0.02 && med < 0.98 ? clamp(Math.log(0.5) / Math.log(med), 0.4, 2.5) : 1; }
    g = num(g, 1);
    const b = clamp(num(o.brightness, 0), -1, 1), c = clamp(num(o.contrast, 0), -1, 1), f = c >= 0 ? 1 / (1 - 0.99 * c) : 1 + c;
    if (g !== 1 || b || c) {
      for (let i = 0; i < v.length; i++) { let q = g !== 1 ? Math.pow(v[i], g) : v[i]; q = (q - 0.5) * f + 0.5 + b; v[i] = q < 0 ? 0 : q > 1 ? 1 : q; }
      yield P(0.3);
    }
    Object.assign(info, contentOf(v));
    const photo = info.content === 'photo';
    const amount = o.sharpen === 'auto' ? (photo ? 1.5 : 0) : clamp(num(o.sharpen, 0), 0, 5);
    if (amount > 0) {
      const bl = yield* blurG(v, W, H, Math.max(0.5, 0.05 * ppm), P(0.3), P(0.7));
      // a soft threshold of 1 % grey: a smooth ramp's 1/255 steps are not sharpened into thin false bands where a level's threshold crosses them
      yield* rows(H, W, (y0, y1) => {
        for (let i = y0 * W, e = y1 * W; i < e; i++) { const d = v[i] - bl[i], a = d < 0 ? -d : d, q = v[i] + amount * (a < 0.01 ? d * a / 0.01 : d); v[i] = q < 0 ? 0 : q > 1 ? 1 : q; }
      }, P(0.7), P(0.72));
    }
    const den = o.denoise === 'auto' ? photo || o.detail < 0.25 : !!o.denoise;
    if (den && W > 2 && H > 2) v = yield* medianG(v, W, H, P(0.7), P(0.95));
    if (o.invert) for (let i = 0; i < v.length; i++) v[i] = 1 - v[i];
    return v;
  }
  /** The working size: the placed image at workPxPerMm, at most maxSide a side and four times the source. */
  function workSize(w, h, o) {
    let s = o.workPxPerMm / o.pxPerMm;
    s = Math.min(s, 4, o.maxSide / Math.max(w, h));
    const W = Math.max(1, Math.round(w * s)), H = Math.max(1, Math.round(h * s));
    return { W, H, sx: W / w, sy: H / h, ppm: o.pxPerMm * Math.sqrt((W / w) * (H / h)) };
  }

  /* ═══ 2 · the levels ═══ */
  /** Multi-level Otsu: the cut of the 256-bin histogram into k classes with the least spread inside them, by dynamic programming. Returns k-1 upper bins. */
  function otsu(hist, k) {
    const L = 256, P0 = new Float64Array(L + 1), P1 = new Float64Array(L + 1), P2 = new Float64Array(L + 1);
    for (let i = 0; i < L; i++) { P0[i + 1] = P0[i] + hist[i]; P1[i + 1] = P1[i] + hist[i] * i; P2[i + 1] = P2[i] + hist[i] * i * i; }
    const sse = (a, b) => { const n = P0[b + 1] - P0[a]; if (n <= 0) return 0; const s = P1[b + 1] - P1[a]; return P2[b + 1] - P2[a] - s * s / n; };
    const D = [], A = [];
    D[0] = new Float64Array(L); for (let b = 0; b < L; b++) D[0][b] = sse(0, b);
    for (let c = 1; c < k; c++) {
      const Dc = D[c] = new Float64Array(L).fill(Infinity), Ac = A[c] = new Int32Array(L), Dp = D[c - 1];
      for (let b = c; b < L; b++) {
        const n1 = P0[b + 1], s1 = P1[b + 1], q1 = P2[b + 1];
        let best = Infinity, ba = c;
        for (let a = c; a <= b; a++) {
          const n = n1 - P0[a], s = s1 - P1[a], q = Dp[a - 1] + (n > 0 ? q1 - P2[a] - s * s / n : 0);
          if (q < best) { best = q; ba = a; }
        }
        Dc[b] = best; Ac[b] = ba;
      }
    }
    const cuts = []; let b = L - 1;
    for (let c = k - 1; c >= 1; c--) { const a = A[c][b]; cuts.unshift(a - 1); b = a - 1; }
    return cuts;
  }
  /** Thresholds 0..1, one per inked level: grey <= t[k] is level k or darker. */
  function thresholdsOf(v, N, how) {
    if (Array.isArray(how) && how.length === N && how.every(t => num(t, null) != null)) return how.map(t => clamp(+t, 0, 1)).sort((a, b) => a - b);
    if (how === 'even') return Array.from({ length: N }, (_, k) => (k + 1) / (N + 1));
    const hist = new Float64Array(256); for (let i = 0; i < v.length; i++) hist[(v[i] * 255 + 0.5) | 0]++;
    return otsu(hist, N + 1).map(b => (b + 0.5) / 255);
  }
  /** Levels 0 (black) .. N (white, no ink), in a frame one pixel wide of white (so every border closes and no walk needs a bounds check). */
  function* labelG(v, W, H, t, p0, p1) {
    const N = t.length, Wp = W + 2, lab = new Uint8Array(Wp * (H + 2)).fill(N), lut = new Uint8Array(1024);
    for (let q = 0; q < 1024; q++) { let c = 0; while (c < N && q / 1023 > t[c]) c++; lut[q] = c; }
    yield* rows(H, W, (y0, y1) => { for (let y = y0; y < y1; y++) for (let x = 0, i = y * W, j = (y + 1) * Wp + 1; x < W; x++) lab[j++] = lut[(v[i++] * 1023 + 0.5) | 0]; }, p0, p1);
    return lab;
  }
  /** Despeckle on the levels. A region (4-connected pixels of one level) joins the level it borders most (ties: the nearer grey) when it is
   *    a speck: at most `area` pixels (the lead, 28 Oct 2026: "despeckle measured in mm² on the charm"), or
   *    a seam: a band at most `thin` pixels wide on average (2 × area / border) lying between a darker and a lighter level. An antialiased edge leaves one
   *    between black and white wherever a threshold crosses it; it would be engraved as a hairline ring round every shape. A thin line with lighter
   *    levels on both sides is a drawn line and stays.
   *  Smallest first. A whole region changes level, so the masks M_k = (level <= k) stay nested. */
  function* despeckleG(lab, W, H, area, thin, p0, p1) {
    if (!(area >= 1) && !(thin > 0)) return 0;
    const Wp = W + 2, n = Wp * (H + 2), seen = new Uint8Array(n), Q = new Int32Array(W * H), pick = [];
    // the frame is seen already: it is no region of its own
    for (let x = 0; x < Wp; x++) { seen[x] = 1; seen[n - 1 - x] = 1; }
    for (let y = 0; y < H + 2; y++) { seen[y * Wp] = 1; seen[y * Wp + Wp - 1] = 1; }
    let qt = 0;
    yield* rows(H, W, (y0, y1) => {
      for (let y = y0; y < y1; y++) for (let i = (y + 1) * Wp + 1, e = i + W; i < e; i++) {
        if (seen[i]) continue;
        // breadth first: the region's pixels end up side by side in Q[s .. qt)
        const l = lab[i], s = qt; let qh = qt, edge = 0, lo = 255, hi = -1;
        Q[qt++] = i; seen[i] = 1;
        while (qh < qt) {
          const p = Q[qh++];
          let q = p - 1, m = lab[q];
          if (m === l) { if (!seen[q]) { seen[q] = 1; Q[qt++] = q; } } else { edge++; if (m < lo) lo = m; if (m > hi) hi = m; }
          q = p + 1; m = lab[q];
          if (m === l) { if (!seen[q]) { seen[q] = 1; Q[qt++] = q; } } else { edge++; if (m < lo) lo = m; if (m > hi) hi = m; }
          q = p - Wp; m = lab[q];
          if (m === l) { if (!seen[q]) { seen[q] = 1; Q[qt++] = q; } } else { edge++; if (m < lo) lo = m; if (m > hi) hi = m; }
          q = p + Wp; m = lab[q];
          if (m === l) { if (!seen[q]) { seen[q] = 1; Q[qt++] = q; } } else { edge++; if (m < lo) lo = m; if (m > hi) hi = m; }
        }
        const c = qt - s;
        if (c <= area || (thin > 0 && lo < l && hi > l && 2 * c <= thin * edge)) pick.push(s, c);
      }
    }, p0, p0 + (p1 - p0) * 0.8);
    const order = []; for (let r = 0; r < pick.length; r += 2) order.push(r);
    order.sort((a, b) => pick[a + 1] - pick[b + 1]);
    yield p0 + (p1 - p0) * 0.9;
    const votes = new Float64Array(16); let merged = 0;
    for (const r of order) {
      const s = pick[r], c = pick[r + 1], own = lab[Q[s]];
      votes.fill(0);
      for (let t = s; t < s + c; t++) {
        const p = Q[t];
        let q = lab[p - 1]; if (q !== own) votes[q]++; q = lab[p + 1]; if (q !== own) votes[q]++;
        q = lab[p - Wp]; if (q !== own) votes[q]++; q = lab[p + Wp]; if (q !== own) votes[q]++;
      }
      let best = -1, bv = 0;
      for (let q = 0; q < 16; q++) if (votes[q] > bv || (votes[q] === bv && bv > 0 && Math.abs(q - own) < Math.abs(best - own))) { bv = votes[q]; best = q; }
      if (best < 0) continue;
      for (let t = s; t < s + c; t++) lab[Q[t]] = best;
      merged++;
    }
    return merged;
  }

  /* ═══ 3 · one border: followed, then fitted (Selinger's method, written from the paper) ═══ */
  const FULLNESS = 0.5;
  let cap = 0, PX, PY, CR, SX, SY, SXX, SXY, SYY, PK, LL, JJ, CL1, COST, PREV, PO, SEG0, SEG1,
    AX, AY, BX, BY, LNX, LNY, LC, ALPHA, CORNER, CONV, ANG, CNT, PEN, BACK, MC, PT, PD;
  function ensure(n) {
    if (n + 4 <= cap) return;
    const keepX = PX, keepY = PY, old = cap;
    cap = Math.max(1024, 1 << Math.ceil(Math.log2(n + 4)));
    PX = new Int32Array(cap); PY = new Int32Array(cap);
    if (keepX) { PX.set(keepX.subarray(0, old)); PY.set(keepY.subarray(0, old)); }
    SX = new Float64Array(cap); SY = new Float64Array(cap); SXX = new Float64Array(cap); SXY = new Float64Array(cap); SYY = new Float64Array(cap);
    PK = new Int32Array(cap); LL = new Int32Array(cap); JJ = new Int32Array(cap); CL1 = new Int32Array(cap);
    COST = new Float64Array(cap); PREV = new Int32Array(cap); PO = new Int32Array(cap); SEG0 = new Int32Array(cap); SEG1 = new Int32Array(cap);
    AX = new Float64Array(cap); AY = new Float64Array(cap); BX = new Float64Array(cap); BY = new Float64Array(cap);
    LNX = new Float64Array(cap); LNY = new Float64Array(cap); LC = new Float64Array(cap); ALPHA = new Float64Array(cap);
    CORNER = new Uint8Array(cap); CONV = new Int8Array(cap); ANG = new Float64Array(cap);
    CR = new Float64Array(cap); CNT = new Int32Array(cap); PEN = new Float64Array(cap); BACK = new Int32Array(cap); MC = new Float64Array(cap * 4);
    PT = new Uint8Array(cap * 4); PD = new Float64Array(cap * 24);
  }
  let walkHash = 0;
  /** Follows one border from vertex (x, y) heading d (0 E, 1 S, 2 W, 3 N; y down) with the ink on the right, until it is back on its first edge. A dark
   *  pixel ahead on the left turns left, so dark pixels touching at a corner stay one shape: the same rule in every mask, so nested masks nest. */
  function walk(lab, Wp, k, x, y, d, base, VIS) {
    const x0 = x, y0 = y, d0 = d;
    let n = 0, hash = 2166136261 | 0;
    do {
      if (n + 2 >= cap) ensure(n + 2);
      PX[n] = x; PY[n] = y; n++;
      if (d === 0) { VIS[base] = 1; x++; base++; }
      else if (d === 1) { y++; base += Wp; }
      else if (d === 2) { VIS[base - 1] = 1; x--; base--; }
      else { y--; base -= Wp; }
      hash = Math.imul(hash ^ d, 16777619);
      let al, ar;
      if (d === 0) { al = base - Wp; ar = base; } else if (d === 1) { al = base; ar = base - 1; }
      else if (d === 2) { al = base - 1; ar = base - Wp - 1; } else { al = base - Wp - 1; ar = base - Wp; }
      if (lab[al] <= k) d = (d + 3) & 3; else if (lab[ar] > k) d = (d + 1) & 3;
    } while (x !== x0 || y !== y0 || d !== d0);
    walkHash = hash;
    return n;
  }

  /** Fits the closed border PX/PY[0..n) and appends its pieces (0 line / 1 cubic, six numbers each: c1, c2, end) to PT/PD from `at`. Returns the count. */
  function fit(n, alphamax, opttol) {
    ensure(n + 2);
    const x0 = PX[0], y0 = PY[0];
    // sums of x, y, x², xy, y² along the border (relative to its first point), point n being point 0 again
    SX[0] = SY[0] = SXX[0] = SXY[0] = SYY[0] = 0;
    for (let i = 0; i <= n; i++) {
      const j = i < n ? i : 0, x = PX[j] - x0, y = PY[j] - y0;
      SX[i + 1] = SX[i] + x; SY[i + 1] = SY[i] + y; SXX[i + 1] = SXX[i] + x * x; SXY[i + 1] = SXY[i] + x * y; SYY[i + 1] = SYY[i] + y * y;
    }
    // and of the shoelace terms, for the area the border encloses between two of its points
    CR[0] = 0;
    for (let i = 0; i < n; i++) { const j = i + 1 < n ? i + 1 : 0; CR[i + 1] = CR[i] + (PX[i] - x0) * (PY[j] - y0) - (PX[j] - x0) * (PY[i] - y0); }
    // PK[i]: the furthest k (i < k < i + n) with every point between within reach of the line i–k and not all four directions used
    for (let i = 0; i < n; i++) {
      const xi = PX[i], yi = PY[i], end = i + n - 1;
      let ax = 0, ay = 0, bx = 0, by = 0, bits = 0, px = xi, py = yi, res = end;
      for (let k = i + 1; k <= end; k++) {
        const kk = k < n ? k : k - n, x = PX[kk], y = PY[kk], dx = x - px, dy = y - py;
        bits |= dx > 0 ? 1 : dx < 0 ? 2 : dy > 0 ? 4 : 8;
        if (bits === 15) { res = k - 1; break; }
        const qx = x - xi, qy = y - yi;
        if (ax * qy - ay * qx < 0 || bx * qy - by * qx > 0) { res = k - 1; break; }
        if (qx > 1 || qx < -1 || qy > 1 || qy < -1) {
          const sx = qx > 0 ? 1 : qx < 0 ? -1 : 0, sy = qy > 0 ? 1 : qy < 0 ? -1 : 0;
          // the pixel square's corners seen furthest clockwise and furthest anticlockwise from point i (the nearer corner when q is on an axis)
          const rx = qx + (sy ? sy : -sx), ry = qy + (sx ? -sx : -sy), lx = qx + (sy ? -sy : -sx), ly = qy + (sx ? sx : -sy);
          if (ax * ry - ay * rx >= 0) { ax = rx; ay = ry; }
          if (bx * ly - by * lx <= 0) { bx = lx; by = ly; }
        }
        px = x; py = y;
      }
      PK[i] = res;
    }
    // LL[i]: the furthest k with the whole run i..k straight = the least PK over the run (unrolled indices)
    LL[n - 1] = PK[n - 1];
    for (let i = n - 2; i >= 0; i--) LL[i] = PK[i] < LL[i + 1] ? PK[i] : LL[i + 1];
    for (let i = n - 1, nx = LL[0] + n; i >= 0; i--) { if (nx < LL[i]) LL[i] = nx; else break; nx = LL[i]; }
    // JJ[i]: the furthest j that segment i→j may reach (its path one point longer at both ends is straight), on the line 0..n
    for (let i = 0; i < n; i++) {
      let j = (i > 0 ? LL[i - 1] : LL[n - 1] - n) - 1;
      if (j > i + n - 3) j = i + n - 3;
      if (j < i + 1) j = i + 1;
      JJ[i] = j < n ? j : n;
    }
    // the optimal polygon through point 0: fewest edges (greedy bounds forward and back), then the least penalty
    let m = 0, a = 0; SEG0[0] = 0;
    while (a < n) { a = JJ[a]; SEG0[++m] = a; }
    for (let j = 1, i = 0; j <= n; j++) { while (JJ[i] < j) i++; CL1[j] = i; }
    SEG1[m] = n; for (let t = m - 1; t >= 0; t--) SEG1[t] = CL1[SEG1[t + 1]];
    COST[0] = 0;
    for (let t = 1; t <= m; t++) {
      for (let j = SEG1[t]; j <= SEG0[t]; j++) {
        let best = Infinity, bk = -1;
        const lo = CL1[j] > SEG1[t - 1] ? CL1[j] : SEG1[t - 1];
        const xj = (j < n ? PX[j] : PX[0]) - x0, yj = (j < n ? PY[j] : PY[0]) - y0;
        for (let k = SEG0[t - 1]; k >= lo; k--) {
          const c = j - k + 1, xk = PX[k] - x0, yk = PY[k] - y0, mx = (xk + xj) / 2, my = (yk + yj) / 2, dx = xj - xk, dy = yj - yk;
          const ex = (SX[j + 1] - SX[k]) / c, ey = (SY[j + 1] - SY[k]) / c;
          const A2 = (SXX[j + 1] - SXX[k]) / c - 2 * mx * ex + mx * mx, B2 = (SXY[j + 1] - SXY[k]) / c - mx * ey - my * ex + mx * my, C2 = (SYY[j + 1] - SYY[k]) / c - 2 * my * ey + my * my;
          const s = C2 * dx * dx - 2 * B2 * dx * dy + A2 * dy * dy, q = COST[k] + (s > 0 ? Math.sqrt(s) : 0);
          if (q < best) { best = q; bk = k; }
        }
        COST[j] = best; PREV[j] = bk;
      }
    }
    if (m < 3) {
      // a speck of a pixel or two has no polygon of its own: its pixel corners
      let r = 0;
      for (let i = 0; i < n; i++) {
        const ip = i ? i - 1 : n - 1, inx = i + 1 < n ? i + 1 : 0;
        if (PX[i] - PX[ip] !== PX[inx] - PX[i] || PY[i] - PY[ip] !== PY[inx] - PY[i]) { PT[r] = 0; PD[r * 6 + 4] = PX[i]; PD[r * 6 + 5] = PY[i]; r++; }
      }
      return r;
    }
    for (let t = m, j = n; t > 0; t--) { j = PREV[j]; PO[t - 1] = j; }
    // each edge's least-squares line (its normal and offset)
    for (let e = 0; e < m; e++) {
      const i = PO[e], j = e + 1 < m ? PO[e + 1] : n, c = j - i + 1;
      const ex = (SX[j + 1] - SX[i]) / c, ey = (SY[j + 1] - SY[i]) / c;
      const A2 = (SXX[j + 1] - SXX[i]) / c - ex * ex, B2 = (SXY[j + 1] - SXY[i]) / c - ex * ey, C2 = (SYY[j + 1] - SYY[i]) / c - ey * ey;
      const l = (A2 + C2) / 2 + Math.sqrt((A2 - C2) * (A2 - C2) / 4 + B2 * B2);
      let vx = l - C2, vy = B2;
      if (B2 * B2 + (l - A2) * (l - A2) > vx * vx + vy * vy) { vx = B2; vy = l - A2; }
      let len = Math.hypot(vx, vy);
      if (len < 1e-9) { vx = (j < n ? PX[j] : PX[0]) - PX[i]; vy = (j < n ? PY[j] : PY[0]) - PY[i]; len = Math.hypot(vx, vy) || 1; }
      const nx = -vy / len, ny = vx / len;
      LNX[e] = nx; LNY[e] = ny; LC[e] = nx * ex + ny * ey;
    }
    // each vertex moved within its pixel (max-distance 1/2) to where the squared distances to its two edges' lines are least
    for (let k = 0; k < m; k++) {
      const vx = PX[PO[k]] - x0, vy = PY[PO[k]] - y0;
      let mxx = 1e-4, mxy = 0, myy = 1e-4, gx = 0, gy = 0;
      for (let s = 0; s < 2; s++) {
        const e = s ? k : (k ? k - 1 : m - 1), nx = LNX[e], ny = LNY[e], d = nx * vx + ny * vy - LC[e];
        mxx += nx * nx; mxy += nx * ny; myy += ny * ny; gx += d * nx; gy += d * ny;
      }
      const det = mxx * myy - mxy * mxy;
      let ux = (mxy * gy - myy * gx) / det, uy = (mxy * gx - mxx * gy) / det;
      if (!(ux >= -0.5 && ux <= 0.5 && uy >= -0.5 && uy <= 0.5)) {
        let best = Infinity;
        for (let s = -0.5; s <= 0.5; s += 1) {
          let y = clamp(-(mxy * s + gy) / myy, -0.5, 0.5), q = mxx * s * s + 2 * mxy * s * y + myy * y * y + 2 * (gx * s + gy * y);
          if (q < best) { best = q; ux = s; uy = y; }
          const x = clamp(-(mxy * s + gx) / mxx, -0.5, 0.5); q = mxx * x * x + 2 * mxy * x * s + myy * s * s + 2 * (gx * x + gy * s);
          if (q < best) { best = q; ux = x; uy = s; }
        }
      }
      AX[k] = vx + ux + x0; AY[k] = vy + uy + y0;
    }
    // corners and curves: segment k runs from B[k-1] to B[k] (the edge midpoints) round vertex A[k]
    for (let k = 0; k < m; k++) { const k1 = k + 1 < m ? k + 1 : 0; BX[k] = (AX[k] + AX[k1]) / 2; BY[k] = (AY[k] + AY[k1]) / 2; }
    for (let k = 0; k < m; k++) {
      const kp = k ? k - 1 : m - 1, p0x = BX[kp], p0y = BY[kp], tx = BX[k] - p0x, ty = BY[k] - p0y, tl = Math.hypot(tx, ty);
      let alpha = 4 / 3;
      if (tl > 1e-12) {
        const d = Math.abs(tx * (AY[k] - p0y) - ty * (AX[k] - p0x)) / tl, h = (Math.abs(tx) + Math.abs(ty)) / tl;
        alpha = d > h ? (4 / 3) * (1 - h / d) : 0;
      }
      if (alpha >= alphamax) { CORNER[k] = 1; ALPHA[k] = 1; CONV[k] = 0; continue; }
      CORNER[k] = 0; ALPHA[k] = alpha < 0.55 ? 0.55 : alpha > 1 ? 1 : alpha;
      {
        // FULLNESS (my own step, not Potrace's): Potrace's alpha comes from the polygon alone, and its curves cut the pixels' convex corners, so a small
        // shape comes out a little thin (a photo's darkest mask lost 2.4 % of its pixels to it, 28 Oct 2026). The alpha that would keep the area the pixels
        // enclose between the two midpoints is worked out from the border's own points, and the curve takes half of the way to it: as smooth to the eye,
        // a third fewer pixels wrong (IoU 0.960 → 0.972 on that mask)
        const Pm = kp === m - 1 && k === 0 ? PO[m - 1] - n : PO[kp], Pk = PO[k], Pn = k + 1 < m ? PO[k + 1] : n;
        let s0 = Math.floor((Pm + Pk) / 2), e0 = Math.floor((Pk + Pn) / 2);
        let pc;
        if (s0 < 0) pc = CR[n] - CR[s0 + n] + CR[e0]; else if (e0 > n) pc = CR[n] - CR[s0] + CR[e0 - n]; else pc = CR[e0] - CR[s0];
        const vs = s0 < 0 ? s0 + n : s0 >= n ? s0 - n : s0, ve = e0 >= n ? e0 - n : e0;
        const bx0 = p0x - x0, by0 = p0y - y0, bx1 = BX[k] - x0, by1 = BY[k] - y0, sx = PX[vs] - x0, sy = PY[vs] - y0, ex = PX[ve] - x0, ey = PY[ve] - y0;
        const Ap = (pc + (bx0 * sy - sx * by0) + (ex * by1 - bx1 * ey) + (bx1 * by0 - bx0 * by1)) / 2;
        const Tk = ((AX[k] - p0x) * (BY[k] - p0y) - (AY[k] - p0y) * (BX[k] - p0x)) / 2;
        if (Math.abs(Tk) > 1e-9) { const r = Ap / Tk; const a2 = r <= 0 ? 0.55 : r >= 1.2 ? 2 : 2 - Math.sqrt(4 - r / 0.3); ALPHA[k] += FULLNESS * ((a2 < 0.55 ? 0.55 : a2 > 1 ? 1 : a2) - ALPHA[k]); }
      }
      const ux = AX[k] - p0x, uy = AY[k] - p0y, wx = BX[k] - AX[k], wy = BY[k] - AY[k], cr = ux * wy - uy * wx;
      CONV[k] = cr > 0 ? 1 : cr < 0 ? -1 : 0;
      ANG[k] = Math.atan2(Math.abs(cr), ux * wx + uy * wy);
    }
    // joined curves: boundary b sits at B[b-1] (boundary 0 = boundary m = B[m-1]); fewest pieces, then least error
    CNT[0] = 0; PEN[0] = 0; for (let b = 1; b <= m; b++) CNT[b] = 1 << 30;
    for (let p = 0; p < m; p++) {
      if (CNT[p] + 1 < CNT[p + 1] || (CNT[p] + 1 === CNT[p + 1] && PEN[p] < PEN[p + 1])) { CNT[p + 1] = CNT[p] + 1; PEN[p + 1] = PEN[p]; BACK[p + 1] = p; }
      if (opttol <= 0 || CORNER[p] || !CONV[p]) continue;
      let turned = ANG[p];
      for (let q = p + 1; q < m; q++) {
        if (CORNER[q] || CONV[q] !== CONV[p]) break;
        turned += ANG[q]; if (turned > 3.1241) break;     // 179°
        const r = joinable(p, q, m, opttol);
        if (r < 0) break;
        const b = q + 1, c = CNT[p] + 1, pen = PEN[p] + r;
        if (c < CNT[b] || (c === CNT[b] && pen < PEN[b])) { CNT[b] = c; PEN[b] = pen; BACK[b] = p; MC[b * 4] = JX1; MC[b * 4 + 1] = JY1; MC[b * 4 + 2] = JX2; MC[b * 4 + 3] = JY2; }
      }
    }
    // the pieces, boundary 0 to m: corners as two lines (A then B), curves as cubics, joined runs as one cubic
    let r = 0;
    const put = (t, a1, a2, a3, a4, x, y, mid) => { PT[r] = t | (mid ? 2 : 0); const o = r * 6; PD[o] = a1; PD[o + 1] = a2; PD[o + 2] = a3; PD[o + 3] = a4; PD[o + 4] = x; PD[o + 5] = y; r++; };
    const stops = []; for (let b = m; b > 0; b = BACK[b]) stops.push(b); stops.reverse();
    let from = 0;
    for (const b of stops) {
      if (b - from === 1) {
        const k = from, kp = k ? k - 1 : m - 1;
        if (CORNER[k]) { put(0, 0, 0, 0, 0, AX[k], AY[k], false); put(0, 0, 0, 0, 0, BX[k], BY[k], true); }
        else { const al = ALPHA[k]; put(1, BX[kp] + al * (AX[k] - BX[kp]), BY[kp] + al * (AY[k] - BY[kp]), BX[k] + al * (AX[k] - BX[k]), BY[k] + al * (AY[k] - BY[k]), BX[k], BY[k], false); }
      } else put(1, MC[b * 4], MC[b * 4 + 1], MC[b * 4 + 2], MC[b * 4 + 3], BX[b - 1], BY[b - 1], false);
      from = b;
    }
    // a midpoint between two straight lines is no node: drop it (start the loop after a piece that stays)
    let keep = -1;
    for (let i = r - 1; i >= 0; i--) if (!((PT[i] & 2) && !(PT[(i + 1) % r] & 1))) { keep = i; break; }
    let out = 0; const base = r;
    for (let s = 1; s <= r; s++) {
      const i = (keep + s) % r;
      if ((PT[i] & 2) && !(PT[(i + 1) % r] & 1)) continue;
      PT[base + out] = PT[i] & 1; for (let t = 0; t < 6; t++) PD[(base + out) * 6 + t] = PD[i * 6 + t];
      out++;
    }
    PT.copyWithin(0, base, base + out); PD.copyWithin(0, base * 6, (base + out) * 6);
    return out;
  }
  let JX1 = 0, JY1 = 0, JX2 = 0, JY2 = 0;
  /** Can segments p..q be one Bézier from B[p-1] to B[q], tangent where they start and end, of the same area, within tol of them? Its error, or -1. */
  function joinable(p, q, m, tol) {
    const pp = p ? p - 1 : m - 1, P0x = BX[pp], P0y = BY[pp], P3x = BX[q], P3y = BY[q];
    const d0x = AX[p] - P0x, d0y = AY[p] - P0y, d3x = AX[q] - P3x, d3y = AY[q] - P3y, den = d0x * d3y - d0y * d3x;
    if (Math.abs(den) < 1e-12) return -1;
    const Dx = P3x - P0x, Dy = P3y - P0y, s = (Dx * d3y - Dy * d3x) / den, t = (Dx * d0y - Dy * d0x) / den;
    if (!(s > 0 && t > 0)) return -1;
    const Ox = P0x + s * d0x, Oy = P0y + s * d0y;
    // the area between the old curves and the chord P0–P3: the polygon of midpoints plus each curve's bulge (3/10 (4α − α²) of its triangle)
    let area = 0;
    for (let k = p; k <= q; k++) {
      const kp = k ? k - 1 : m - 1, al = ALPHA[k];
      area += ((BX[kp] - P0x) * (BY[k] - P0y) - (BY[kp] - P0y) * (BX[k] - P0x)) / 2;
      area += ((AX[k] - BX[kp]) * (BY[k] - BY[kp]) - (AY[k] - BY[kp]) * (BX[k] - BX[kp])) / 2 * 0.3 * (4 * al - al * al);
    }
    const T = ((Ox - P0x) * (P3y - P0y) - (Oy - P0y) * (P3x - P0x)) / 2;
    if (!(Math.abs(T) > 1e-12)) return -1;
    const ratio = area / T;
    if (!(ratio > 0 && ratio <= 1.2)) return -1;
    const alpha = 2 - Math.sqrt(4 - ratio / 0.3);
    const c1x = P0x + alpha * (Ox - P0x), c1y = P0y + alpha * (Oy - P0y), c2x = P3x + alpha * (Ox - P3x), c2y = P3y + alpha * (Oy - P3y);
    let pen = 0;
    // the point of the new curve whose tangent runs along (dx, dy)
    const along = (dx, dy) => {
      const u0 = (c1x - P0x) * dy - (c1y - P0y) * dx, u1 = (c2x - c1x) * dy - (c2y - c1y) * dx, u2 = (P3x - c2x) * dy - (P3y - c2y) * dx;
      const qa = u0 - 2 * u1 + u2, qb = 2 * (u1 - u0), qc = u0;
      let tt = -1;
      if (Math.abs(qa) < 1e-12) { if (Math.abs(qb) > 1e-12) tt = -qc / qb; }
      else {
        const disc = qb * qb - 4 * qa * qc; if (disc < 0) return -1;
        const sq = Math.sqrt(disc), r1 = (-qb + sq) / (2 * qa), r2 = (-qb - sq) / (2 * qa);
        tt = r1 >= -1e-9 && r1 <= 1 + 1e-9 ? r1 : r2;
      }
      if (!(tt >= -1e-9 && tt <= 1 + 1e-9)) return -1;
      const u = 1 - tt;
      JZX = u * u * u * P0x + 3 * u * u * tt * c1x + 3 * u * tt * tt * c2x + tt * tt * tt * P3x;
      JZY = u * u * u * P0y + 3 * u * u * tt * c1y + 3 * u * tt * tt * c2y + tt * tt * tt * P3y;
      return 0;
    };
    // at every inner midpoint B[k] the old curve touches the line A[k]–A[k+1]: the new one must touch it within tol, between A[k] and A[k+1]
    for (let k = p; k < q; k++) {
      const dx = AX[k + 1] - AX[k], dy = AY[k + 1] - AY[k], dl = Math.hypot(dx, dy);
      if (dl < 1e-12 || along(dx, dy) < 0) return -1;
      const d = (dx * (JZY - AY[k]) - dy * (JZX - AX[k])) / dl;
      if (Math.abs(d) > tol) return -1;
      const pr = ((JZX - AX[k]) * dx + (JZY - AY[k]) * dy) / (dl * dl);
      if (pr < 0 || pr > 1) return -1;
      pen += d * d;
    }
    // at every old curve's apex the new one must not cut inside it by more than tol
    for (let k = p; k <= q; k++) {
      const kp = k ? k - 1 : m - 1, al = ALPHA[k], dx = BX[k] - BX[kp], dy = BY[k] - BY[kp], dl = Math.hypot(dx, dy);
      if (dl < 1e-12 || along(dx, dy) < 0) return -1;
      const ax = (BX[kp] + BX[k] + 3 * (BX[kp] + al * (AX[k] - BX[kp])) + 3 * (BX[k] + al * (AX[k] - BX[k]))) / 8;
      const ay = (BY[kp] + BY[k] + 3 * (BY[kp] + al * (AY[k] - BY[kp])) + 3 * (BY[k] + al * (AY[k] - BY[k]))) / 8;
      const side = dx * (AY[k] - ay) - dy * (AX[k] - ax) >= 0 ? 1 : -1;
      const d = side * (dx * (JZY - ay) - dy * (JZX - ax)) / dl;
      if (d < -tol) return -1;
      pen += d * d;
    }
    JX1 = c1x; JY1 = c1y; JX2 = c2x; JY2 = c2y;
    return pen;
  }
  let JZX = 0, JZY = 0;

  /* ═══ 4 · a level: every border of M_k, fitted and kept ═══ */
  function store() { return { n: 0, start: new Int32Array(256), count: new Int32Array(256), sx: new Float64Array(256), sy: new Float64Array(256), edge: new Int32Array(256),
    len: new Int32Array(256), hash: new Int32Array(256), match: new Uint8Array(256), np: 0, pt: new Uint8Array(4096), pd: new Float64Array(4096 * 6) }; }
  function grow(a, n) { const b = new a.constructor(Math.max(n, a.length * 2)); b.set(a); return b; }
  function* traceLevelG(lab, W, H, k, VIS, o, st, p0, p1) {
    const Wp = W + 2;
    VIS.fill(0); st.n = 0; st.np = 0;
    let work = 0;
    for (let y = 0; y <= H; y++) {
      for (let x = 0, idx = (y + 1) * Wp + 1; x < W; x++, idx++) {
        const below = lab[idx] <= k;
        if (below === (lab[idx - Wp] <= k) || VIS[idx]) continue;
        let n = walk(lab, Wp, k, below ? x : x + 1, y, below ? 0 : 2, below ? idx : idx + 1, VIS);
        if (!below && n > 1) {                         // a hole's walk starts one step right of its top-left corner: start on the corner
          const fx = PX[0], fy = PY[0]; PX.copyWithin(0, 1, n); PY.copyWithin(0, 1, n); PX[n - 1] = fx; PY[n - 1] = fy;
        }
        const hash = walkHash;
        const r = fit(n, o.alphamax, o.opttol);
        work += n;
        let c = st.n;
        if (c >= st.start.length) for (const key of ['start', 'count', 'sx', 'sy', 'edge', 'len', 'hash', 'match']) st[key] = grow(st[key], c + 1);
        if (st.np + r > st.pt.length) { st.pt = grow(st.pt, st.np + r); st.pd = grow(st.pd, (st.np + r) * 6); }
        st.start[c] = st.np; st.count[c] = r; st.sx[c] = PD[(r - 1) * 6 + 4]; st.sy[c] = PD[(r - 1) * 6 + 5];
        st.edge[c] = idx; st.len[c] = n; st.hash[c] = hash; st.match[c] = 0;
        st.pt.set(PT.subarray(0, r), st.np); st.pd.set(PD.subarray(0, r * 6), st.np * 6);
        st.np += r; st.n = c + 1;
      }
      work += W >> 3;                                  // the scan itself counts too
      if (work > PATH_PTS) { work = 0; yield p0 + (p1 - p0) * (y / H); }
    }
  }
  /** Writes one border into a packed layer: forward, or reversed (as a hole of the band above it); x, y scaled back to source pixels. */
  function emit(L, st, c, rev, kx, ky, W, H) {
    const s = st.start[c], r = st.count[c], pt = st.pt, pd = st.pd;
    need(L, r + 2, r * 6 + 2);
    // in source pixels, and never outside the picture: a control point past its edge is pulled onto it (the curve, inside its control points, follows)
    const X = v => (v <= 0 ? 0 : v >= W ? W / kx : v / kx), Y = v => (v <= 0 ? 0 : v >= H ? H / ky : v / ky);
    L.cmd[L.nc++] = 0; L.xy[L.nx++] = X(st.sx[c]); L.xy[L.nx++] = Y(st.sy[c]);
    if (!rev) {
      for (let i = 0; i < r; i++) {
        const o = (s + i) * 6;
        if (pt[s + i] === 0) { if (i === r - 1) break; L.cmd[L.nc++] = 1; L.xy[L.nx++] = X(pd[o + 4]); L.xy[L.nx++] = Y(pd[o + 5]); }
        else { L.cmd[L.nc++] = 2; L.xy[L.nx++] = X(pd[o]); L.xy[L.nx++] = Y(pd[o + 1]); L.xy[L.nx++] = X(pd[o + 2]); L.xy[L.nx++] = Y(pd[o + 3]); L.xy[L.nx++] = X(pd[o + 4]); L.xy[L.nx++] = Y(pd[o + 5]); }
      }
    } else {
      for (let i = r - 1; i >= 0; i--) {
        const o = (s + i) * 6, tx = i ? pd[o - 2] : st.sx[c], ty = i ? pd[o - 1] : st.sy[c];
        if (pt[s + i] === 0) { if (i === 0) break; L.cmd[L.nc++] = 1; L.xy[L.nx++] = X(tx); L.xy[L.nx++] = Y(ty); }
        else { L.cmd[L.nc++] = 2; L.xy[L.nx++] = X(pd[o + 2]); L.xy[L.nx++] = Y(pd[o + 3]); L.xy[L.nx++] = X(pd[o]); L.xy[L.nx++] = Y(pd[o + 1]); L.xy[L.nx++] = X(tx); L.xy[L.nx++] = Y(ty); }
      }
    }
    L.cmd[L.nc++] = 3;
  }
  function need(L, c, x) {
    if (L.nc + c > L.cmd.length) L.cmd = grow(L.cmd, L.nc + c);
    if (L.nx + x > L.xy.length) L.xy = grow(L.xy, L.nx + x);
  }

  /* ═══ 5 · the whole trace, as a generator (the Worker runs it straight through; the page in slices) ═══ */
  function* runG(img, opts) {
    const o = options(opts), t0 = now(), ms = {};
    const w0 = img.width | 0, h0 = img.height | 0, ch = channelsOf(img);
    let v = yield* luminanceG(img.data, w0, h0, ch, 0, 0.02);
    const ws = workSize(w0, h0, o), W = ws.W, H = ws.H;
    v = yield* resampleG(v, w0, h0, W, H, 0.02, 0.06);
    const info = {};
    v = yield* prepG(v, W, H, o, ws.ppm, info, 0.06, 0.16);
    ms.prep = now() - t0;
    const N = o.levels, t = thresholdsOf(v, N, o.thresholds);
    const lab = yield* labelG(v, W, H, t, 0.16, 0.18);
    const raw = o.keepLabels ? unframe(lab, W, H) : null;
    const speck = Math.round(o.speckMm2 * ws.ppm * ws.ppm);
    const seam = o.speckMm2 > 0 ? SEAM_MM * ws.ppm : 0;
    const merged = yield* despeckleG(lab, W, H, speck, seam, 0.18, 0.22);
    ms.posterise = now() - t0 - ms.prep;
    // pixels per level (the posterised area of each band, source px²)
    const counts = new Float64Array(N + 1), Wp = W + 2;
    for (let y = 1; y <= H; y++) for (let i = y * Wp + 1, e = i + W; i < e; i++) counts[lab[i]]++;
    const VIS = new Uint8Array(Wp * (H + 2));
    yield 0.22;
    let prev = null, cur = store(), spare = store();
    const layers = [], span = 0.75 / N;
    for (let k = 0; k < N; k++) {
      yield* traceLevelG(lab, W, H, k, VIS, o, cur, 0.22 + span * k, 0.22 + span * (k + 1));
      const L = { level: k, grey: k / N, cmd: new Uint8Array(1024), xy: new Float32Array(4096), nc: 0, nx: 0, areaPx: counts[k] / (ws.sx * ws.sy) };
      if (o.stack) L.areaPx = Array.from(counts.subarray(0, k + 1)).reduce((a, b) => a + b, 0) / (ws.sx * ws.sy);
      if (prev && !o.stack) {
        // a border the same in both masks bounds nothing of this band: it cancels
        const at = new Map(); for (let c = 0; c < prev.n; c++) at.set(prev.edge[c], c);
        for (let c = 0; c < cur.n; c++) {
          const q = at.get(cur.edge[c]);
          if (q !== undefined && prev.len[q] === cur.len[c] && prev.hash[q] === cur.hash[c]) { cur.match[c] = 1; prev.match[q] = 1; }
        }
      }
      const pe = 0.22 + span * (k + 1);
      for (let c = 0; c < cur.n; c++) { if (!cur.match[c]) emit(L, cur, c, false, ws.sx, ws.sy, W, H); if ((c & 2047) === 2047) yield pe; }
      if (prev && !o.stack) for (let c = 0; c < prev.n; c++) { if (!prev.match[c]) emit(L, prev, c, true, ws.sx, ws.sy, W, H); if ((c & 2047) === 2047) yield pe; }
      for (let c = 0; c < cur.n; c++) cur.match[c] = 0;
      L.cmd = L.cmd.slice(0, L.nc); L.xy = L.xy.slice(0, L.nx);
      let nodes = 0; for (let i = 0; i < L.nc; i++) if (L.cmd[i] !== 3) nodes++;
      L.nodes = nodes; delete L.nc; delete L.nx;
      if (nodes) layers.push(L);
      const used = prev; prev = cur; cur = used || spare;
    }
    ms.trace = now() - t0 - ms.prep - ms.posterise;
    ms.work = now() - t0;
    const res = { width: w0, height: h0, levels: N, thresholds: t.map(x => Math.round(x * 1000) / 1000), greys: Array.from({ length: N }, (_, k) => k / N),
      suggestLevels: info.suggestLevels, content: info.content, midtones: info.midtones, scale: ws.sx, work: { width: W, height: H, pxPerMm: ws.ppm, speckPx: speck, merged },
      mode: o.stack ? 'stack' : 'bands', layers, ms };
    if (o.keepLabels) { res.work.labels = unframe(lab, W, H); res.work.labelsRaw = raw; }
    return res;
  }
  function unframe(lab, W, H) { const out = new Uint8Array(W * H), Wp = W + 2; for (let y = 0; y < H; y++) out.set(lab.subarray((y + 1) * Wp + 1, (y + 1) * Wp + 1 + W), y * W); return out; }
  /** The packed layers as the editor's subpaths (rounded to 1/1000 px), in slices. */
  function* unpackG(res, p0, p1) {
    const r3 = v => Math.round(v * 1000) / 1000, total = res.layers.reduce((a, L) => a + L.cmd.length, 0) || 1;
    let done = 0;
    for (const L of res.layers) {
      const subpaths = []; let sub = null, j = 0, since = 0;
      for (let i = 0; i < L.cmd.length; i++) {
        const c = L.cmd[i];
        if (c === 0) { sub = [['m', [r3(L.xy[j]), r3(L.xy[j + 1])]]]; subpaths.push(sub); j += 2; }
        else if (c === 1) { sub.push(['l', [r3(L.xy[j]), r3(L.xy[j + 1])]]); j += 2; }
        else if (c === 2) { sub.push(['c', [r3(L.xy[j]), r3(L.xy[j + 1])], [r3(L.xy[j + 2]), r3(L.xy[j + 3])], [r3(L.xy[j + 4]), r3(L.xy[j + 5])]]); j += 6; }
        else sub.push(['h']);
        if (++since >= 4096) { done += since; since = 0; yield p0 + (p1 - p0) * done / total; }
      }
      done += since;
      L.subpaths = subpaths; delete L.cmd; delete L.xy;
    }
    res.nodes = res.layers.reduce((a, L) => a + L.nodes, 0);
    return res;
  }

  /* ═══ 6 · the Worker (this same file, from a Blob URL) ═══ */
  if (inWorker) {
    self.onmessage = e => {
      const m = e.data || {};
      if (m.type !== 'trace') return;
      try {
        const g = runG(m.img, m.opts); let r, last = now();
        while (!(r = g.next()).done) if (now() - last > 40) { last = now(); self.postMessage({ type: 'progress', id: m.id, p: r.value }); }
        const res = r.value, moving = [];
        for (const L of res.layers) moving.push(L.cmd.buffer, L.xy.buffer);
        self.postMessage({ type: 'done', id: m.id, res }, moving);
      } catch (err) { self.postMessage({ type: 'error', id: m.id, message: String(err && err.message || err) }); }
    };
    self.postMessage({ type: 'ready' });
    return null;
  }

  /* ═══ 7 · the page's side ═══ */
  const cancelled = () => { const e = new Error('Trace cancelled'); e.name = 'AbortError'; e.cancelled = true; return e; };
  let channel = null; const queue = [];
  /** The next slice: a MessageChannel task in a browser (no 4 ms timer clamp; input and painting run between), setImmediate in node. */
  function later(fn) {
    if (typeof setImmediate === 'function') setImmediate(fn);
    else if (typeof MessageChannel === 'function') {
      if (!channel) { channel = new MessageChannel(); channel.port1.onmessage = () => { const f = queue.shift(); if (f) f(); }; }
      queue.push(fn); channel.port2.postMessage(0);
    } else setTimeout(fn, 0);
  }
  /** Runs a generator on this thread in slices of SLICE_MS, telling `job` its progress; rejects once job.cancelled. */
  function sliced(g, job, p0, p1) {
    return new Promise((resolve, reject) => {
      const tick = () => {
        if (job.cancelled) { try { g.return(); } catch (_) { /* gone */ } reject(cancelled()); return; }
        const t = now(); let r;
        try { do { r = g.next(); if (!r.done && typeof r.value === 'number') job.p = r.value; } while (!r.done && now() - t < SLICE_MS); }
        catch (err) { reject(err); return; }
        const took = now() - t; if (took > job.longest) job.longest = took;
        job.slices++;
        if (r.done) { resolve(r.value); return; }
        job.tell(p0 + (p1 - p0) * job.p);
        later(tick);
      };
      later(tick);
    });
  }
  let worker = null, workerUrl = null, broken = false, ready = false, seq = 0;
  const jobs = new Map();
  function workerFor() {
    if (broken || worker) return worker;
    if (!(root && root.Worker && root.Blob && root.URL && root.URL.createObjectURL)) { broken = true; return null; }
    try {
      if (!workerUrl) workerUrl = root.URL.createObjectURL(new root.Blob(['(' + charmNestTraceFactory.toString() + ')(self, true);'], { type: 'text/javascript' }));
      const wk = new root.Worker(workerUrl);
      ready = false;
      wk.onmessage = ev => fromWorker(wk, ev.data || {});
      wk.onerror = ev => { if (ev && ev.preventDefault) ev.preventDefault(); workerFailed(wk, ev && ev.message); };
      worker = wk;
    } catch (_) { broken = true; worker = null; }
    return worker;
  }
  function fromWorker(wk, m) {
    if (m.type === 'ready') { ready = true; return; }
    const job = jobs.get(m.id); if (!job || job.wk !== wk) return;
    if (m.type === 'progress') { job.tell(0.95 * m.p); return; }
    if (m.type === 'error') { finish(job, Promise.reject(new Error(m.message))); return; }
    if (m.type === 'done') { job.wk = null; finish(job, sliced(unpackG(m.res, 0.95, 1), job, 0, 1).then(res => (res.worker = true, res))); }
  }
  /** A Worker that fails before it says it is ready cannot start here (a policy, an old browser): its jobs and every later one run on the page. */
  function workerFailed(wk, msg) {
    if (wk !== worker) return;
    const early = !ready; worker = null; try { wk.terminate(); } catch (_) { /* gone */ }
    if (early) broken = true;
    for (const job of Array.from(jobs.values())) if (job.wk === wk) {
      job.wk = null;
      if (early) onPage(job); else finish(job, Promise.reject(new Error('Trace failed: ' + (msg || 'the worker stopped'))));
    }
  }
  function onPage(job) {
    const p = sliced(runG(job.img, job.opts), job, 0, 0.95).then(res => sliced(unpackG(res, 0.95, 1), job, 0, 1)).then(res => (res.worker = false, res));
    finish(job, p);
  }
  function finish(job, p) {
    p.then(res => {
      if (job.cancelled) throw cancelled();
      res.ms.total = now() - job.t0; res.ms.longestSliceOnPage = Math.round(job.longest * 10) / 10;
      jobs.delete(job.id); job.tell(1); job.resolve(res);
    }).catch(err => { jobs.delete(job.id); job.reject(job.cancelled ? cancelled() : err); });
  }
  function trace(src, opts) {
    opts = Object.assign({}, opts || {});
    const onProgress = typeof opts.onProgress === 'function' ? opts.onProgress : null, signal = opts.signal;
    delete opts.onProgress; delete opts.signal;
    return new Promise((resolve, reject) => {
      let ch;
      try { ch = channelsOf(src); } catch (e) { reject(e); return; }
      if (signal && signal.aborted) { reject(cancelled()); return; }
      const id = ++seq, job = { id, t0: now(), resolve, reject, opts, cancelled: false, p: 0, longest: 0, slices: 0, wk: null, last: -1,
        img: { data: src.data, width: src.width | 0, height: src.height | 0 },
        tell(p) { if (onProgress && p > job.last) { job.last = p; try { onProgress(Math.min(1, p)); } catch (_) { /* the caller's */ } } } };
      jobs.set(id, job);
      if (signal && signal.addEventListener) signal.addEventListener('abort', () => cancelJob(job), { once: true });
      const wk = workerFor();
      if (wk) {
        // the caller keeps its pixels: the Worker gets a copy, moved rather than copied again
        const data = new (src.data.constructor === Uint8ClampedArray ? Uint8ClampedArray : Uint8Array)(src.data);
        job.wk = wk;
        try { wk.postMessage({ type: 'trace', id, img: { data, width: job.img.width, height: job.img.height, channels: ch }, opts }, [data.buffer]); }
        catch (_) { job.wk = null; onPage(job); }
      } else onPage(job);
    });
  }
  function cancelJob(job) {
    if (job.cancelled) return;
    job.cancelled = true;
    if (job.wk && job.wk === worker) {
      const others = Array.from(jobs.values()).filter(j => j !== job && j.wk === worker && !j.cancelled);
      if (!others.length) { try { worker.terminate(); } catch (_) { /* gone */ } worker = null; }
    }
    if (job.wk) { jobs.delete(job.id); job.reject(cancelled()); }
  }
  /** Stops every trace, running or waiting. */
  function cancel() {
    const all = Array.from(jobs.values());
    if (worker) { try { worker.terminate(); } catch (_) { /* gone */ } worker = null; }
    for (const job of all) { job.cancelled = true; if (job.wk) { jobs.delete(job.id); job.reject(cancelled()); } }
    return all.length;
  }
  function drain(g) { let r; while (!(r = g.next()).done); return r.value; }
  function traceSync(src, opts) {
    const t = now(), res = drain(unpackG(drain(runG(src, Object.assign({}, opts || {}))), 0, 1));
    res.ms.total = now() - t; res.worker = false; return res;
  }
  /** The grey preview at the source size (or opts.maxSide), as ImageData. */
  function prepared(src, opts, side) {
    const o = options(opts), ch = channelsOf(src), w0 = src.width | 0, h0 = src.height | 0;
    let v = drain(luminanceG(src.data, w0, h0, ch, 0, 1));
    const s = Math.min(1, (side || Infinity) / Math.max(w0, h0)), W = Math.max(1, Math.round(w0 * s)), H = Math.max(1, Math.round(h0 * s));
    v = drain(resampleG(v, w0, h0, W, H, 0, 1));
    const info = {};
    v = drain(prepG(v, W, H, o, o.pxPerMm * (W / w0), info, 0, 1));
    return { v, W, H, o, info, ppm: o.pxPerMm * (W / w0) };
  }
  function imageOf(W, H, fill) {
    const data = new Uint8ClampedArray(W * H * 4);
    for (let i = 0, j = 0; i < W * H; i++, j += 4) { const g = fill(i); data[j] = data[j + 1] = data[j + 2] = g; data[j + 3] = 255; }
    return typeof ImageData === 'function' ? new ImageData(data, W, H) : { data, width: W, height: H };
  }
  function grey(src, opts) {
    const p = prepared(src, opts, opts && opts.maxSide);
    return imageOf(p.W, p.H, i => Math.round(p.v[i] * 255));
  }
  function posterise(src, opts) {
    const p = prepared(src, opts, num(opts && opts.maxSide, 512)), N = p.o.levels, t = thresholdsOf(p.v, N, p.o.thresholds);
    const lab = drain(labelG(p.v, p.W, p.H, t, 0, 1));
    drain(despeckleG(lab, p.W, p.H, Math.round(p.o.speckMm2 * p.ppm * p.ppm), p.o.speckMm2 > 0 ? SEAM_MM * p.ppm : 0, 0, 1));
    const flat = unframe(lab, p.W, p.H);
    return { image: imageOf(p.W, p.H, i => (flat[i] >= N ? 255 : Math.round(255 * flat[i] / N))), width: p.W, height: p.H, levels: N,
      thresholds: t.map(x => Math.round(x * 1000) / 1000), greys: Array.from({ length: N }, (_, k) => k / N), suggestLevels: p.info.suggestLevels, content: p.info.content, midtones: p.info.midtones };
  }
  function suggest(src, opts) {
    const p = prepared(src, Object.assign({}, opts, { sharpen: 0, denoise: false }), 256);
    return { levels: p.info.suggestLevels, content: p.info.content, midtones: p.info.midtones };
  }
  return { trace, cancel, grey, posterise, suggest, traceSync, defaults: DEFAULTS,
    get busy() { return jobs.size; }, get usesWorker() { return !broken && !!(root && root.Worker); },
    _core: { options, otsu, thresholdsOf, runG, unpackG, contentOf } };
});
