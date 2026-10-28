/* charm-nest-ce-image.js — AN IMAGE ON A CHARM, in the Charm Editor (Design & Engrave, item 11; Paul, 28 Oct 2026: "I also need the ability for users to import images into this
   environment, and to manipulate the image and place it on a charm. All images must be converted to a vector format while keeping the quality of the image and the colour should be a
   greyscale using the black as base").

   One importer of the Charm Editor (charm-nest-charm-editor.js CharmEditor.importers: its Import button, a file dropped on the work area, a file pasted): PNG, JPG and WebP. A drawing
   (.ai .pdf .svg .dxf) is the vector importer's (charm-nest-ce-import.js). The file opens in the IMAGE STEP, a card over the editor in its own design language (the studio's upload card,
   C 1134-1145, its range controls, C 6200-6247, its magic eraser and tolerance, S 2262-2265): turn 90° either way, flip, crop, take the background away (a click floods the area round it
   to white: white is never engraved); the Photoshop prep every laser guide does, in its order (the lead's research, 28 Oct 2026, §4.2): luminance, a 3 × 3 median (denoise), an unsharp
   mask (sharpen), auto-levels (0.5 % clipped at each end), gamma, brightness and contrast, invert; and the trace itself: greys or LINE ART (a local-mean threshold, Bradley's, for phone
   photos of handwriting and signatures in uneven light), levels (1-8, 4 to start; 1 is plain black; the picture's own suggestion offered beside it), detail, smoothness, despeckle. The
   panel says plainly that a photo becomes a posterised, stencil-like engraving in N greys. The preview is split in three, original | greyscale | vector (drag the two dividers, or show one
   whole): the greyscale is made here at once on every change (a copy at most 560 px: the filters kept between tone changes, which are one lookup table, about a millisecond), the vector
   from a copy at most 280 px traced in CharmNestTrace's worker (charm-nest-ce-trace.js), the newest asked for as soon as the last is back, so a slider dragged never queues up work.
   PLACE traces the image at the charm's own scale (24 px a mm, 1400 px at most) in the worker with a progress bar and puts it on the charm as ONE group: a filled path per grey level,
   role hatch, fill [g, g, g] (black the darkest, white not engraved; the trace's fixed greys, k / N for band k of N, so a grey is the same laser layer from job to job), marked
   `artGrey` so the grey stays grey in the editor, in the piece's record (charm-nest-art-edit.js), on the server
   (charmNestLibrary.js artPut) and in the laser's file (charm-nest-pdf.js artOps), so the operator gives each grey its own power (the lead's D2). It is centred and fitted inside the
   charm's outline (the biggest box of the picture's shape that stays 0.4 mm inside every cut line), or into the box of the part selected. From there it is an ordinary group: moved,
   scaled, turned, ungrouped, edited. The group keeps what it was made from (`trace`: the settings, and the source by id in this page's memory), so RE-TRACE (the Object menu) opens
   the Image step again with those settings and puts the new trace exactly where the old one is now, moved, scaled and turned as it is. A saved piece's parts come back flat with the
   group's id (their `group`), and Re-trace finds the source by it while this page holds it.
   What a piece's record can hold is kept to (charmNestLibrary.js ART_MAX: a path at most 200 KB, an edit at most 60 000 operators and 600 KB): a trace over that is made again smaller,
   and said so.

   UMD: window.CharmNestCeImage in the page (it registers itself with CharmEditor), module.exports in Node (the pure parts, for the tests). Nothing here talks to the cloud. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestCeImage = api;
  if (root && root.document) api.register();
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const PT = 72 / 25.4, MM = 25.4 / 72;
  const ACCEPT = ['.png', '.jpg', '.jpeg', '.webp', 'image/png', 'image/jpeg', 'image/webp'];
  const DEF = Object.freeze({ mode: 'grey', levels: 4, detail: 0.5, smooth: 0.5, despeckle: 2, invert: false, auto: true, gamma: 1, brightness: 0, contrast: 0, sharpen: 0, denoise: false,
    rot: 0, flipH: false, flipV: false, crop: null });
  const SRC_MAX = 2400;        // the source kept, px on its long side (a phone's photo is 4000: more than any charm can show)
  const SHOW_MAX = 560;        // the preview's original and greyscale, px
  const PREVIEW_MAX = 280;     // the preview's vector: traced from this, px
  const FULL_PX_MM = 20, FULL_MAX = 1400;   // the trace on Place: 20 px a mm on the charm (the trace's own working pitch, 0.05 mm, a fibre laser's hatch), at most 1400 px; never enlarged
  const MARGIN_MM = 0.4;       // an image placed in the outline stays this far inside every cut line
  const BUDGET = { layerBytes: 180000, ops: 40000, bytes: 450000 };   // (charmNestLibrary.js ART_MAX: d 200 000, ops 60 000, bytes 600 000 for the whole edit; room left for the rest of it)
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
  const r4 = v => Math.round(v * 1e4) / 1e4;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const log = (kind, data) => { try { if (root && root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event(kind, data); } catch (e) { /* the log is the log's business */ } };
  const tracer = () => (root && root.CharmNestTrace && typeof root.CharmNestTrace.trace === 'function' ? root.CharmNestTrace : null);
  const now = () => (root && root.performance ? root.performance.now() : Date.now());
  let seq = 0;
  const newId = () => 'img' + Date.now().toString(36) + (++seq).toString(36);

  /* ═══ 1 · the settings ═══ */
  /** Settings made whole and in range (anything missing is the default). */
  function norm(o) {
    o = Object.assign({}, DEF, o || {});
    const c = Array.isArray(o.crop) && o.crop.length === 4 && o.crop.every(fin) ? o.crop.map(v => clamp(v, 0, 1)) : null;
    const n01 = (v, d) => clamp(fin(+v) ? +v : d, 0, 1), npm = v => clamp(fin(+v) ? +v : 0, -1, 1);
    return { mode: o.mode === 'line' ? 'line' : 'grey', levels: clamp(Math.round(+o.levels || DEF.levels), 1, 8), detail: n01(o.detail, DEF.detail), smooth: n01(o.smooth, DEF.smooth),
      despeckle: clamp(Math.round(fin(+o.despeckle) ? +o.despeckle : DEF.despeckle), 0, 10), invert: !!o.invert, auto: o.auto !== false, gamma: clamp(fin(+o.gamma) && +o.gamma > 0 ? +o.gamma : 1, 0.3, 3),
      brightness: npm(o.brightness), contrast: npm(o.contrast), sharpen: n01(o.sharpen, 0), denoise: !!o.denoise,
      rot: ((Math.round((+o.rot || 0) / 90) * 90) % 360 + 360) % 360, flipH: !!o.flipH, flipV: !!o.flipV,
      crop: c && c[2] - c[0] > 0.01 && c[3] - c[1] > 0.01 && !(c[0] === 0 && c[1] === 0 && c[2] === 1 && c[3] === 1) ? c : null };
  }
  /** The levels the trace is asked for: line art is one (black), greys their own. */
  const levelsOf = o => (o.mode === 'line' ? 1 : o.levels);
  /** What CharmNestTrace.trace is asked (charm-nest-ce-trace.js): the picture comes already prepared here (prepGrey: what the Greyscale pane shows is what is traced), so the trace's own
   *  prep is left out; pxPerMm says how big it is on the charm, so despeckle is an area on the metal. */
  const traceOpts = (o, pxPerMm) => ({ levels: levelsOf(o), detail: o.detail, smooth: o.smooth, despeckle: o.despeckle, invert: false, autoLevels: false, gamma: 1, contrast: 0, brightness: 0,
    sharpen: 0, denoise: false, adaptive: false, pxPerMm: fin(pxPerMm) && pxPerMm > 0 ? pxPerMm : 20 });
  const KIND = { line: 'line art', illustration: 'an illustration', photo: 'a photo' };
  /** A quarter turn (dir 1 clockwise, -1 the other way) of what is shown: the crop turns with the picture, and a flip made before it is the other flip after it (R·H = V·R). */
  function turn(o, dir) {
    o = norm(o); const c = o.crop;
    return norm(Object.assign({}, o, { rot: o.rot + (dir < 0 ? 270 : 90), flipH: o.flipV, flipV: o.flipH,
      crop: c ? (dir < 0 ? [c[1], 1 - c[2], c[3], 1 - c[0]] : [1 - c[3], c[0], 1 - c[1], c[2]]) : null }));
  }
  /** A flip of what is shown, left to right (h) or top to bottom; the crop follows. */
  function flip(o, h) {
    o = norm(o); const c = o.crop;
    return norm(Object.assign({}, o, h ? { flipH: !o.flipH, crop: c ? [1 - c[2], c[1], 1 - c[0], c[3]] : null } : { flipV: !o.flipV, crop: c ? [c[0], 1 - c[3], c[2], 1 - c[1]] : null }));
  }
  /** The source's size as shown (a quarter turn swaps it) and the part of it kept (px). */
  const shownSize = (w, h, o) => (o.rot % 180 ? [h, w] : [w, h]);
  function keptSize(w, h, o) { const [sw, sh] = shownSize(w, h, o), c = o.crop || [0, 0, 1, 1]; return [Math.max(1, (c[2] - c[0]) * sw), Math.max(1, (c[3] - c[1]) * sh)]; }
  /** The matrix [a,b,c,d,e,f] (canvas order) that draws the source (w × h) as shown: turned, then flipped. */
  function shownMatrix(w, h, o) {
    const [sw, sh] = shownSize(w, h, o);
    const R = o.rot === 90 ? [0, 1, -1, 0, h, 0] : o.rot === 180 ? [-1, 0, 0, -1, w, h] : o.rot === 270 ? [0, -1, 1, 0, 0, w] : [1, 0, 0, 1, 0, 0];
    const F = [o.flipH ? -1 : 1, 0, 0, o.flipV ? -1 : 1, o.flipH ? sw : 0, o.flipV ? sh : 0];
    return mul(F, R);
  }
  const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
  /** w × h scaled to at most `max` on its long side (never up). */
  function fitPx(w, h, max) { const k = Math.min(1, max / Math.max(w, h)); return [Math.max(1, Math.round(w * k)), Math.max(1, Math.round(h * k))]; }

  /* ═══ 2 · greyscale with black as the base (the lead's D2), prepared as a laser guide prepares a picture (the research, §4.2) ═══ */
  /** THE PREP: luminance (0.299 / 0.587 / 0.114; a transparent pixel is white paper) → 3 × 3 median (o.denoise) → unsharp mask (o.sharpen 0..1: amount up to 3 over a 1 px box blur)
   *  → auto-levels (o.auto: 0.5 % clipped at each end) → gamma → brightness and contrast → invert. In line-art mode (o.mode 'line') the toned grey is thresholded against its own
   *  neighbourhood (Bradley's local mean, a window an eighth of the picture; brightness moves the threshold): ink where it is darker than around it, so a page lit unevenly still reads.
   *  px RGBA (w × h). cache (optional, the preview's): the filtered base is kept while only the tone changes, so a tone slider is one lookup table. Self-contained: it is also the body
   *  of the worker below. → { g: Uint8ClampedArray (a grey a pixel), mid (the share of mid-tones after auto-levels), top4 (the share of the four fullest of 32 bins), bins } */
  function prepGrey(px, w, h, o, cache) {
    const n = w * h, amt = (+o.sharpen || 0) * 3, key = w + 'x' + h + ':' + (o.denoise ? 1 : 0) + ':' + Math.round(amt * 100);   // (filterKey below says the same)
    if (cache && cache.src !== px) { cache.src = px; cache.luma = null; cache.key = null; cache.base = null; }
    let base = cache && cache.key === key ? cache.base : null;
    if (!base) {
      let a = cache && cache.luma;
      if (!a) {
        a = new Uint8ClampedArray(n);
        for (let i = 0, p = 0; i < n; i++, p += 4) { const al = px[p + 3], y = (px[p] * 77 + px[p + 1] * 150 + px[p + 2] * 29) >> 8; a[i] = al === 255 ? y : 255 - (((255 - y) * al + 127) / 255 | 0); }
        if (cache) cache.luma = a;
      }
      let b = a;
      if (o.denoise) {   // (Paeth's 19 exchanges: the median of nine, in locals)
        const out = new Uint8ClampedArray(n);
        for (let y = 0; y < h; y++) {
          const r0 = (y > 0 ? y - 1 : 0) * w, r1 = y * w, r2 = (y < h - 1 ? y + 1 : y) * w;
          for (let x = 0; x < w; x++) {
            const l = x > 0 ? x - 1 : 0, r = x < w - 1 ? x + 1 : x;
            let p0 = b[r0 + l], p1 = b[r0 + x], p2 = b[r0 + r], p3 = b[r1 + l], p4 = b[r1 + x], p5 = b[r1 + r], p6 = b[r2 + l], p7 = b[r2 + x], p8 = b[r2 + r], t;
            if (p1 > p2) { t = p1; p1 = p2; p2 = t; } if (p4 > p5) { t = p4; p4 = p5; p5 = t; } if (p7 > p8) { t = p7; p7 = p8; p8 = t; } if (p0 > p1) { t = p0; p0 = p1; p1 = t; } if (p3 > p4) { t = p3; p3 = p4; p4 = t; }
            if (p6 > p7) { t = p6; p6 = p7; p7 = t; } if (p1 > p2) { t = p1; p1 = p2; p2 = t; } if (p4 > p5) { t = p4; p4 = p5; p5 = t; } if (p7 > p8) { t = p7; p7 = p8; p8 = t; } if (p0 > p3) { t = p0; p0 = p3; p3 = t; }
            if (p5 > p8) { t = p5; p5 = p8; p8 = t; } if (p4 > p7) { t = p4; p4 = p7; p7 = t; } if (p3 > p6) { t = p3; p3 = p6; p6 = t; } if (p1 > p4) { t = p1; p1 = p4; p4 = t; } if (p2 > p5) { t = p2; p2 = p5; p5 = t; }
            if (p4 > p7) { t = p4; p4 = p7; p7 = t; } if (p4 > p2) { t = p4; p4 = p2; p2 = t; } if (p6 > p4) { t = p6; p6 = p4; p4 = t; } if (p4 > p2) { t = p4; p4 = p2; p2 = t; }
            out[r1 + x] = p4;
          }
        }
        b = out;
      }
      if (amt > 0) {
        const hs = new Uint16Array(n), out = new Uint8ClampedArray(n);
        for (let y = 0; y < h; y++) { const r = y * w; for (let x = 0; x < w; x++) hs[r + x] = b[r + (x > 0 ? x - 1 : 0)] + b[r + x] + b[r + (x < w - 1 ? x + 1 : x)]; }
        for (let y = 0; y < h; y++) {
          const r0 = (y > 0 ? y - 1 : 0) * w, r1 = y * w, r2 = (y < h - 1 ? y + 1 : y) * w;
          for (let x = 0; x < w; x++) { const v = b[r1 + x], blur = (hs[r0 + x] + hs[r1 + x] + hs[r2 + x]) / 9; out[r1 + x] = v + amt * (v - blur); }
        }
        b = out;
      }
      base = b;
      if (cache) { cache.key = key; cache.base = base; }
    }
    const hist = new Uint32Array(256); for (let i = 0; i < n; i++) hist[base[i]]++;
    let lo = 0, hi = 255;
    if (o.auto !== false) { const clip = n * 0.005; let s = 0; while (lo < 250 && (s += hist[lo]) <= clip) lo++; s = 0; while (hi > lo + 4 && (s += hist[hi]) <= clip) hi--; }
    const g = 1 / Math.max(0.05, +o.gamma || 1), c = +o.contrast || 0, k = c >= 0 ? 1 / Math.max(0.02, 1 - c) : 1 + c, br = (+o.brightness || 0) * 0.5, line = o.mode === 'line';
    const lut = new Uint8ClampedArray(256), lev = new Uint8ClampedArray(256);
    for (let v = 0; v < 256; v++) {
      const x0 = Math.min(1, Math.max(0, (v - lo) / Math.max(1, hi - lo))); lev[v] = Math.round(x0 * 255);
      let x = Math.pow(x0, g); x = (x - 0.5) * k + 0.5 + (line ? 0 : br); x = Math.min(1, Math.max(0, x));
      lut[v] = Math.round((o.invert && !line ? 1 - x : x) * 255);
    }
    const out = new Uint8ClampedArray(n);
    for (let i = 0; i < n; i++) out[i] = lut[base[i]];
    if (line) {   // (Bradley & Roth: the sum of the window from an integral image; ink when darker than its mean by t)
      const I = new Float64Array((w + 1) * (h + 1)), W1 = w + 1, s = Math.max(4, Math.round(Math.max(w, h) / 16)), t = Math.min(0.45, Math.max(0.02, 0.15 + (+o.brightness || 0) * 0.12));
      for (let y = 0; y < h; y++) { let row = 0; for (let x = 0; x < w; x++) { row += out[y * w + x]; I[(y + 1) * W1 + x + 1] = I[y * W1 + x + 1] + row; } }
      for (let y = 0; y < h; y++) {
        const y0 = Math.max(0, y - s), y1 = Math.min(h, y + s + 1);
        for (let x = 0; x < w; x++) {
          const x0 = Math.max(0, x - s), x1 = Math.min(w, x + s + 1), cnt = (x1 - x0) * (y1 - y0), sum = I[y1 * W1 + x1] - I[y0 * W1 + x1] - I[y1 * W1 + x0] + I[y0 * W1 + x0];
          const ink = out[y * w + x] * cnt < sum * (1 - t);
          out[y * w + x] = (ink !== !!o.invert) ? 0 : 255;
        }
      }
    }
    // what the picture is (after auto-levels, before the person's own tone): the mid-tones' share and how few tones hold it (the research §4.1)
    let mid = 0; const bins = new Float64Array(32);
    for (let v = 0; v < 256; v++) { const L = lev[v], m = hist[v] / n; if (L >= 51 && L <= 204) mid += m; bins[L >> 3] += m; }
    const top4 = Array.from(bins).sort((a, b) => b - a).slice(0, 4).reduce((a, b) => a + b, 0);
    return { g: out, mid, top4, bins: Array.from(bins) };
  }
  /** The key prepGrey keeps its filtered base under (the same words as in it: prepGrey is a worker's body and can see nothing outside itself). */
  const filterKey = (w, h, o) => w + 'x' + h + ':' + (o.denoise ? 1 : 0) + ':' + Math.round((+o.sharpen || 0) * 3 * 100);
  /** The levels the picture asks for (the research §4.1): little mid-tone is line art, one level (plain black); a few flat tones an illustration, as many greys as it has (2 to 4);
   *  anything else a photo, five (stylised). The trace's own suggestion wins when it gives one (CharmNestTrace.suggest). → { levels, kind } */
  function suggestLevels(p) {
    if (!p) return null;
    if (p.mid < 0.15) return { levels: 1, kind: 'line art' };
    if (p.top4 > 0.85) { const k = (p.bins || []).filter((m, i) => i < 31 && m >= 0.03).length; return { levels: clamp(k, 2, 4), kind: 'an illustration' }; }
    return { levels: 5, kind: 'a photo' };
  }
  /** The prep as one picture of grey RGBA (the trace's input): { data, width, height }. */
  function greyImage(px, w, h, o) {
    const r = prepGrey(px, w, h, o), d = new Uint8ClampedArray(w * h * 4);
    for (let i = 0, p = 0; i < r.g.length; i++, p += 4) { d[p] = d[p + 1] = d[p + 2] = r.g[i]; d[p + 3] = 255; }
    return { data: d, width: w, height: h };
  }
  /** Kept for callers of the first version: grey with brightness, contrast and invert only (no filters, no auto-levels). */
  function grey(src, o, dst) {
    const r = prepGrey(src.data, src.width, src.height, Object.assign({}, norm(o), { auto: false, sharpen: 0, denoise: false, mode: 'grey' })), d = (dst || src).data;
    for (let i = 0, p = 0; i < r.g.length; i++, p += 4) { d[p] = d[p + 1] = d[p + 2] = r.g[i]; d[p + 3] = 255; }
    return dst || src;
  }
  /* the full-size prep off the page (a 1400 px picture's median and sharpen are 60-100 ms: never on the page's own thread), in a worker of its own made from prepGrey's own text */
  let prepW = null, prepSeq = 0; const prepWait = new Map();
  function prepWorker() {
    if (prepW !== null) return prepW || null;
    try {
      const code = `const prepGrey = ${prepGrey.toString()};\nself.onmessage = e => { const m = e.data; try { const r = prepGrey(m.px, m.w, m.h, m.o), d = new Uint8ClampedArray(m.w * m.h * 4);`
        + ` if (m.base) { self.postMessage({ id: m.id, d: r.g }, [r.g.buffer]); return; }`
        + ` for (let i = 0, p = 0; i < r.g.length; i++, p += 4) { d[p] = d[p + 1] = d[p + 2] = r.g[i]; d[p + 3] = 255; } self.postMessage({ id: m.id, d }, [d.buffer]); } catch (er) { self.postMessage({ id: m.id, error: String(er && er.message || er) }); } };`;
      prepW = new root.Worker(URL.createObjectURL(new Blob([code], { type: 'text/javascript' })));
      prepW.onmessage = e => { const w = prepWait.get(e.data.id); if (!w) return; prepWait.delete(e.data.id); if (e.data.error) w.rej(new Error(e.data.error)); else w.res(e.data.d); };
      prepW.onerror = () => { for (const w of prepWait.values()) w.rej(new Error('the picture could not be prepared')); prepWait.clear(); prepW = false; };
    } catch (e) { prepW = false; }
    return prepW || null;
  }
  /** The filtered base of a picture (its median and unsharp mask, no tone: prepGrey with the tone left as it is) off the page's thread; null when no worker can be made. */
  function baseAsync(px, w, h, o) {
    const W = root && root.Worker && root.Blob && root.URL ? prepWorker() : null;
    if (!W) return null;
    const id = ++prepSeq, flat = Object.assign({}, o, { auto: false, gamma: 1, brightness: 0, contrast: 0, invert: false, mode: 'grey' });
    return new Promise((res, rej) => { prepWait.set(id, { res, rej }); const copy = px.slice(); W.postMessage({ id, px: copy, w, h, o: flat, base: true }, [copy.buffer]); });
  }
  /** greyImage off the page when it can be (the page's own thread when no worker can be made). */
  function greyImageAsync(px, w, h, o) {
    const W = root && root.Worker && root.Blob && root.URL ? prepWorker() : null;
    if (!W) return Promise.resolve(greyImage(px, w, h, o));
    const id = ++prepSeq;
    return new Promise((res, rej) => { prepWait.set(id, { res: d => res({ data: d, width: w, height: h }), rej }); W.postMessage({ id, px, w, h, o: Object.assign({}, o) }, [px.buffer]); });
  }

  /* ═══ 3 · where it goes on the charm ═══ */
  /** Polylines (flat x,y arrays, closed) of subpaths, cubics cut into chords. */
  function polysOf(sps) {
    const out = [];
    for (const sub of sps || []) {
      let pts = [], cur = null;
      for (const op of sub || []) {
        const k = op && op[0];
        if (k === 'm') { if (pts.length >= 6) out.push(pts); pts = [op[1][0], op[1][1]]; cur = op[1]; }
        else if (k === 'l') { pts.push(op[1][0], op[1][1]); cur = op[1]; }
        else if (k === 'c' && cur) {
          const a = cur, p1 = op[1], p2 = op[2], p3 = op[3];
          for (let i = 1; i <= 12; i++) { const t = i / 12, u = 1 - t; pts.push(u * u * u * a[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t * t * t * p3[0], u * u * u * a[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t * t * t * p3[1]); }
          cur = p3;
        }
      }
      if (pts.length >= 6) out.push(pts);
    }
    return out;
  }
  const boxOfPolys = polys => { let b = null; for (const p of polys) for (let i = 0; i < p.length; i += 2) b = b ? [Math.min(b[0], p[i]), Math.min(b[1], p[i + 1]), Math.max(b[2], p[i]), Math.max(b[3], p[i + 1])] : [p[i], p[i + 1], p[i], p[i + 1]]; return b; };
  function inside(x, y, polys) {
    let w = false;
    for (const p of polys) for (let i = 0, n = p.length / 2, j = n - 1; i < n; j = i++) { const yi = p[2 * i + 1], yj = p[2 * j + 1]; if ((yi > y) !== (yj > y) && x < (p[2 * j] - p[2 * i]) * (y - yi) / (yj - yi) + p[2 * i]) w = !w; }
    return w;
  }
  function distTo(x, y, polys) {
    let best = Infinity;
    for (const p of polys) for (let i = 0, n = p.length / 2, j = n - 1; i < n; j = i++) {
      const ax = p[2 * j], ay = p[2 * j + 1], dx = p[2 * i] - ax, dy = p[2 * i + 1] - ay, L = dx * dx + dy * dy;
      const t = L ? clamp(((x - ax) * dx + (y - ay) * dy) / L, 0, 1) : 0, d = Math.hypot(x - ax - t * dx, y - ay - t * dy); if (d < best) best = d;
    }
    return best;
  }
  /** The biggest box of aspect `ar` (w / h) that stays `margin` inside the outline `outer` (polylines, even-odd: its own holes are holes) and outside every other cut line `holes`,
   *  centred on the outline's box centre or its centroid (whichever holds the bigger box). → [x0, y0, x1, y1] or null. */
  function fitInOutline(ar, outer, holes, margin) {
    const ob = boxOfPolys(outer); if (!ob || !(ar > 0)) return null;
    const all = outer.concat(holes || []);
    const W = ob[2] - ob[0], H = ob[3] - ob[1], w0 = Math.min(W, H * ar), h0 = w0 / ar;
    const ok = (cx, cy, s) => {
      const hw = w0 * s / 2, hh = h0 * s / 2, x0 = cx - hw, x1 = cx + hw, y0 = cy - hh, y1 = cy + hh, N = 16;
      for (let i = 0; i <= N; i++) {
        const t = i / N, pts = [[x0 + (x1 - x0) * t, y0], [x0 + (x1 - x0) * t, y1], [x0, y0 + (y1 - y0) * t], [x1, y0 + (y1 - y0) * t]];
        for (const [x, y] of pts) { if (!inside(x, y, outer) || (holes || []).some(h => inside(x, y, [h])) || distTo(x, y, all) < margin) return false; }
      }
      for (const p of all) for (let i = 0; i < p.length; i += 2) if (p[i] > x0 - margin && p[i] < x1 + margin && p[i + 1] > y0 - margin && p[i + 1] < y1 + margin) return false;
      return true;
    };
    const centroid = () => { let a = 0, cx = 0, cy = 0; const p = outer.reduce((m, q) => (q.length > m.length ? q : m), outer[0]); for (let i = 0, n = p.length / 2, j = n - 1; i < n; j = i++) { const f = p[2 * j] * p[2 * i + 1] - p[2 * i] * p[2 * j + 1]; a += f; cx += (p[2 * j] + p[2 * i]) * f; cy += (p[2 * j + 1] + p[2 * i + 1]) * f; } return Math.abs(a) > 1e-9 ? [cx / (3 * a), cy / (3 * a)] : null; };
    let best = null;
    for (const c of [[(ob[0] + ob[2]) / 2, (ob[1] + ob[3]) / 2], centroid()]) {
      if (!c) continue;
      let lo = 0, hi = 1;
      if (!ok(c[0], c[1], 1e-3)) continue;
      for (let k = 0; k < 18; k++) { const m = (lo + hi) / 2; if (ok(c[0], c[1], m)) lo = m; else hi = m; }
      if (!best || lo > best.s) best = { s: lo, c };
    }
    if (!best) return null;
    const hw = w0 * best.s / 2, hh = h0 * best.s / 2;
    return [best.c[0] - hw, best.c[1] - hh, best.c[0] + hw, best.c[1] + hh];
  }
  /** A box of aspect ar fitted (centred) inside the box b. */
  function fitInBox(ar, b) { const W = b[2] - b[0], H = b[3] - b[1], w = Math.min(W, H * ar), h = w / ar, cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2; return [cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2]; }
  /** Where an image of aspect ar goes in the editor (ctx: the extension points' ctx): { box, into: 'selection' | 'outline' | 'frame', what } — the selected parts' box when `useSel`
   *  and something is selected, else inside the charm's outline (its biggest cut line), else the frame. */
  function target(ctx, ar, useSel) {
    const geo = ctx.geo, M = ctx.model, sel = ctx.selection();
    if (useSel && sel.length) {
      let b = null;
      for (const id of sel) { const e = ctx.entry(id); const d = e && geo.drawn(e.it, e.P); if (d) b = b ? [Math.min(b[0], d[0]), Math.min(b[1], d[1]), Math.max(b[2], d[2]), Math.max(b[3], d[3])] : d.slice(); }
      if (b && b[2] - b[0] > 0.05 && b[3] - b[1] > 0.05) return { box: fitInBox(ar, b), into: 'selection', what: sel.length === 1 ? 'the selected part' : 'the selected parts' };
    }
    const cuts = [];
    M.walk(ctx.items(), it => { if (it.kind !== 'group' && it.role === 'cut' && !it.hidden) { const e = ctx.entry(it.id); const polys = polysOf(M.outlinesOf(it, geo, e ? e.P : M.I)); const b = boxOfPolys(polys); if (b) cuts.push({ polys, area: (b[2] - b[0]) * (b[3] - b[1]) }); } });
    cuts.sort((a, b) => b.area - a.area);
    if (cuts.length) {
      const box = fitInOutline(ar, cuts[0].polys, [].concat(...cuts.slice(1).map(c => c.polys)), MARGIN_MM * PT);
      if (box) return { box, into: 'outline', what: "the charm's outline" };
    }
    const f = ctx.frame(), m = Math.min(f.wPt, f.hPt) * 0.06;
    return { box: fitInBox(ar, [f.x0 + m, f.y0 + m, f.x0 + f.wPt - m, f.y0 + f.hPt - m]), into: 'frame', what: 'the charm' };
  }

  /* ═══ 4 · the trace as the editor's parts ═══ */
  const greyName = g => (g <= 0.0005 ? 'Black' : `Grey ${Math.round((1 - g) * 100)}%`);
  /** How big the parts would be in a piece's record: { ops, bytes, worstLayer } (charmNestLibrary.js counts M L C Z; art-edit writes 3 decimals). */
  function sizeOf(layers) {
    let ops = 0, bytes = 0, worst = 0;
    for (const L of layers || []) {
      let b = 0;
      for (const sub of L.subpaths || []) for (const op of sub || []) { ops++; b += op[0] === 'c' ? 49 : op[0] === 'h' ? 1 : 17; }
      bytes += b; worst = Math.max(worst, b);
    }
    return { ops, bytes, worstLayer: worst };
  }
  const fits = s => s.ops <= BUDGET.ops && s.bytes <= BUDGET.bytes && s.worstLayer <= BUDGET.layerBytes;
  /** The trace (px, y down) laid into the box (face pt) as one group: a filled path per grey level, lightest at the bottom, black on top. `trace` is what it was made from. */
  function buildGroup(res, box, opts) {
    opts = opts || {};
    const gid = opts.id || opts.src || newId(), k = Math.min((box[2] - box[0]) / res.width, (box[3] - box[1]) / res.height);
    const ox = (box[0] + box[2]) / 2 - res.width * k / 2, oy = (box[1] + box[3]) / 2 - res.height * k / 2;
    const P = p => [r4(ox + p[0] * k), r4(oy + p[1] * k)];
    const layers = (res.layers || []).filter(L => L && Array.isArray(L.subpaths) && L.subpaths.length && fin(+L.grey) && +L.grey < 0.999).slice().sort((a, b) => b.grey - a.grey);
    const used = new Set();
    const children = layers.map(L => {
      const g = Math.round(clamp(+L.grey, 0, 1) * 1000) / 1000;
      let key = String(Math.round(g * 100)); while (used.has(key)) key += 'b'; used.add(key);
      const sps = L.subpaths.map(sub => sub.map(op => (op[0] === 'm' || op[0] === 'l' ? [op[0], P(op[1])] : op[0] === 'c' ? ['c', P(op[1]), P(op[2]), P(op[3])] : ['h'])));
      return { id: gid + '.g' + key, kind: 'path', role: 'hatch', source: 'added', name: greyName(g), hidden: false, locked: false, fill: [g, g, g], stroke: false, artGrey: true, even: true, evenOdd: true,
        subpaths: sps, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 };
    });
    const t = Object.assign({ src: opts.src || gid, box: box.map(r4) }, opts.settings ? norm(opts.settings) : {}, opts.name ? { name: String(opts.name).slice(0, 80) } : {});
    return { id: gid, kind: 'group', role: 'hatch', source: 'added', name: 'Image' + (opts.name ? ' · ' + String(opts.name).replace(/\.[a-z0-9]+$/i, '').slice(0, 40) : ''), hidden: false, locked: false,
      tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, trace: t, children };
  }
  /** The traced set the selection is in: a group that keeps its `trace`, or (a piece saved and opened again: its parts come back flat) the grey parts that share its `group`.
   *  → { kind: 'group', id, src, settings, box } | { kind: 'flat', ids, src, box } | null */
  function traceSet(ctx) {
    const sel = ctx.selection(); if (!sel.length) return null;
    for (const id of sel) {
      const e = ctx.entry(id); if (!e) continue;
      let it = e.it;
      if (it.kind === 'group' && it.trace && it.trace.src) return { kind: 'group', id: it.id, src: String(it.trace.src), settings: norm(it.trace), box: Array.isArray(it.trace.box) ? it.trace.box : null, it };
      if (it.artGrey && e.parent && e.parent.trace) { it = e.parent; return { kind: 'group', id: it.id, src: String(it.trace.src), settings: norm(it.trace), box: Array.isArray(it.trace.box) ? it.trace.box : null, it }; }
      if (it.artGrey && it.group) {
        const gid = String(it.group), ids = [];
        ctx.model.walk(ctx.items(), x => { if (x.artGrey && String(x.group || '') === gid) ids.push(x.id); });
        return { kind: 'flat', ids, src: gid, settings: null, box: null };
      }
    }
    return null;
  }

  /* ═══ 5 · the sources this page holds (Re-trace): the decoded picture by id, the newest dozen ═══ */
  const SOURCES = new Map();
  function keepSource(id, s) { SOURCES.delete(id); SOURCES.set(id, s); while (SOURCES.size > 12) SOURCES.delete(SOURCES.keys().next().value); }

  /* ═══ 6 · the page: decoding, drawing, the Image step ═══ */
  const doc = root && root.document ? root.document : null;
  const RD = { willReadFrequently: true };   // (a canvas read back with getImageData is kept in memory, not on the graphics card: a read is then a copy, not a wait)
  function canvas(w, h) { const c = doc.createElement('canvas'); c.width = Math.max(1, Math.round(w)); c.height = Math.max(1, Math.round(h)); return c; }
  /** A file as a canvas at most SRC_MAX px (its own pixels when smaller). */
  async function decode(file) {
    let bmp = null;
    try { if (root.createImageBitmap) bmp = await root.createImageBitmap(file); } catch (e) { bmp = null; }
    if (!bmp) {
      const url = URL.createObjectURL(file);
      try { bmp = await new Promise((res, rej) => { const im = new Image(); im.onload = () => res(im); im.onerror = () => rej(new Error('not a picture this browser can read')); im.src = url; }); }
      finally { setTimeout(() => URL.revokeObjectURL(url), 1000); }
    }
    const w0 = bmp.width || bmp.naturalWidth, h0 = bmp.height || bmp.naturalHeight;
    if (!(w0 > 0 && h0 > 0)) throw new Error('the picture is empty');
    const [w, h] = fitPx(w0, h0, SRC_MAX), c = canvas(w, h), g = c.getContext('2d', RD);
    g.imageSmoothingQuality = 'high'; g.drawImage(bmp, 0, 0, w, h);
    if (bmp.close) bmp.close();
    return { canvas: c, w, h, w0, h0, name: String(file.name || 'image'), type: String(file.type || '') };
  }
  /** The source as shown and kept (turned, flipped, cropped), drawn on white into a canvas of tw × th. `whole`: the whole picture as shown, the crop ignored. */
  function drawShown(dst, src, o, whole) {
    const g = dst.getContext('2d', RD), [sw, sh] = shownSize(src.w, src.h, o), c = whole || !o.crop ? [0, 0, 1, 1] : o.crop;
    const kx = dst.width / ((c[2] - c[0]) * sw), ky = dst.height / ((c[3] - c[1]) * sh);
    g.setTransform(1, 0, 0, 1, 0, 0); g.fillStyle = '#fff'; g.fillRect(0, 0, dst.width, dst.height);
    const M = mul([kx, 0, 0, ky, -c[0] * sw * kx, -c[1] * sh * ky], shownMatrix(src.w, src.h, o));
    g.setTransform(M[0], M[1], M[2], M[3], M[4], M[5]); g.imageSmoothingEnabled = true; g.imageSmoothingQuality = 'high';
    g.drawImage(src.canvas, 0, 0); g.setTransform(1, 0, 0, 1, 0, 0);
    return dst;
  }
  /** The kept picture prepared (prepGrey) as the trace's input, at most `max` px; off the page's thread when it can be. */
  function greyPixels(src, o, max) {
    const [kw, kh] = keptSize(src.w, src.h, o), [w, h] = fitPx(kw, kh, max), c = drawShown(canvas(w, h), src, o);
    return greyImageAsync(c.getContext('2d').getImageData(0, 0, w, h).data, w, h, o);
  }
  const inv = m => { const d = m[0] * m[3] - m[1] * m[2]; return [m[3] / d, -m[1] / d, -m[2] / d, m[0] / d, (m[2] * m[5] - m[3] * m[4]) / d, (m[1] * m[4] - m[0] * m[5]) / d]; };
  /** THE BACKGROUND TAKEN AWAY (the studio's magic eraser, S 2262: "How close a shade counts"): the area joined to source pixel (x, y) whose colour is within `tol` (0..100) of it, made
   *  white paper, so it is never engraved. The picture as it came is kept for Restore. → the number of pixels made white. */
  function eraseAt(src, x, y, tol) {
    x = Math.floor(x); y = Math.floor(y);
    if (x < 0 || y < 0 || x >= src.w || y >= src.h) return 0;
    const g = src.canvas.getContext('2d', RD), img = g.getImageData(0, 0, src.w, src.h), d = img.data, W = src.w, H = src.h;
    if (!src.orig) src.orig = g.getImageData(0, 0, W, H);
    const p0 = (y * W + x) * 4, r0 = d[p0], g0 = d[p0 + 1], b0 = d[p0 + 2], a0 = d[p0 + 3], T = clamp(+tol || 0, 0, 100) * 2.55;
    const near = p => Math.max(Math.abs(d[p] - r0), Math.abs(d[p + 1] - g0), Math.abs(d[p + 2] - b0), Math.abs(d[p + 3] - a0)) <= T;
    const seen = new Uint8Array(W * H), stack = new Int32Array(W * H + 1); let sp = 0, n = 0;
    stack[sp++] = y * W + x; seen[y * W + x] = 1;
    while (sp) {   // (a scanline fill: each popped pixel runs left and right, the rows above and below pushed where they start)
      const i = stack[--sp], yy = (i / W) | 0; let l = i - yy * W, r = l;
      while (l > 0 && !seen[yy * W + l - 1] && near((yy * W + l - 1) * 4)) { l--; seen[yy * W + l] = 1; }
      while (r < W - 1 && !seen[yy * W + r + 1] && near((yy * W + r + 1) * 4)) { r++; seen[yy * W + r] = 1; }
      for (let xx = l; xx <= r; xx++) {
        const q = (yy * W + xx) * 4; d[q] = d[q + 1] = d[q + 2] = d[q + 3] = 255; n++;
        if (yy > 0) { const j = (yy - 1) * W + xx; if (!seen[j] && near(j * 4)) { seen[j] = 1; stack[sp++] = j; } }
        if (yy < H - 1) { const j = (yy + 1) * W + xx; if (!seen[j] && near(j * 4)) { seen[j] = 1; stack[sp++] = j; } }
      }
    }
    g.putImageData(img, 0, 0);
    return n;
  }
  function restoreSource(src) { if (src && src.orig) { src.canvas.getContext('2d').putImageData(src.orig, 0, 0); src.orig = null; return true; } return false; }
  const dOf = sps => { let d = ''; const f = v => Math.round(v * 100) / 100; for (const sub of sps || []) for (const op of sub || []) { if (op[0] === 'm' || op[0] === 'l') d += (op[0] === 'm' ? 'M' : 'L') + f(op[1][0]) + ' ' + f(op[1][1]); else if (op[0] === 'c') d += 'C' + [op[1], op[2], op[3]].map(p => f(p[0]) + ' ' + f(p[1])).join(' '); else if (op[0] === 'h') d += 'Z'; } return d; };
  const rgb = g => { const v = Math.round(clamp(g, 0, 1) * 255); return `rgb(${v},${v},${v})`; };

  const SV = d => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${d}</svg>`;
  const IC = {
    left: SV('<path d="M4 9h9.5a5.5 5.5 0 0 1 0 11H9"/><path d="m8 5-4 4 4 4"/>'),
    right: SV('<path d="M20 9h-9.5a5.5 5.5 0 0 0 0 11H15"/><path d="m16 5 4 4-4 4"/>'),
    flipH: SV('<path d="M12 3v18" stroke-dasharray="2 2.4"/><path d="M9 6 3 18h6Z"/><path d="m15 6 6 12h-6Z"/>'),
    flipV: SV('<path d="M3 12h18" stroke-dasharray="2 2.4"/><path d="M6 9 18 3v6Z"/><path d="m6 15 12 6v-6Z"/>'),
    crop: SV('<path d="M6 2v15a1 1 0 0 0 1 1h15"/><path d="M2 6h15a1 1 0 0 1 1 1v15"/>'),
    // (the studio's magic eraser: a wand and its spark)
    erase: SV('<path d="m4 20 11-11"/><path d="m13 7 4 4"/><path d="M18 3v3"/><path d="M21 6h-3"/><path d="M15.5 3.5 17 5"/><path d="M21 9.5 19.5 8"/>'),
    image: SV('<rect x="3.5" y="4.5" width="17" height="15" rx="1"/><circle cx="9" cy="10" r="1.8"/><path d="m4 18 5.5-5 4 3.5 2.5-2 4 3.5"/>')
  };
  const CSS = `
.cei{ position:absolute; inset:0; z-index:42; background:rgba(255,255,255,.62); display:flex; align-items:center; justify-content:center; padding:16px; }
.cei__card{ position:relative; width:min(1020px,100%); height:min(680px,100%); display:grid; grid-template-columns:minmax(0,1fr) 284px; grid-template-rows:auto minmax(0,1fr) auto; background:#fff;
  border:1px solid var(--ink); box-shadow:0 24px 64px rgba(28,29,29,.26); outline:none; animation:ceiIn .18s cubic-bezier(.16,1,.3,1); }
@keyframes ceiIn{ from{ opacity:0; transform:translateY(6px) scale(.99); } to{ opacity:1; transform:none; } }
.cei__head{ grid-area:1/1/2/3; display:flex; align-items:baseline; gap:14px; padding:14px 18px 11px; border-bottom:1px solid var(--hair); min-width:0; }
.cei__head h3{ margin:0; font-family:var(--serif); font-weight:500; font-size:22px; line-height:1.1; color:var(--ink); flex:none; }
.cei__eyebrow{ font-size:10px; font-weight:700; letter-spacing:.2em; text-transform:uppercase; color:#8a8784; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; min-width:0; }
.cei__view{ grid-area:2/1/3/2; display:flex; flex-direction:column; min-width:0; min-height:0; padding:12px 14px 12px 18px; gap:10px; }
.cei__modes{ display:flex; align-items:center; gap:10px; flex:none; min-height:30px; }
.cei__modes .ce-seg button{ padding:0 10px; }
.cei__hint{ font-size:10px; letter-spacing:.12em; text-transform:uppercase; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; min-width:0; }
.cei__tol{ display:flex; align-items:center; gap:8px; min-width:0; flex:1 1 auto; font-size:11px; color:var(--ink); }
.cei__tol input{ flex:1 1 auto; min-width:60px; max-width:180px; }
.cei__tol output{ font-weight:700; min-width:20px; }
.cei__tol .ce-prop{ height:26px; }
.cei__room{ position:relative; flex:1 1 auto; min-height:0; background:var(--band); border:1px solid var(--hair); display:flex; align-items:center; justify-content:center; overflow:hidden; touch-action:none; }
.cei__sheet{ position:relative; background:#fff; box-shadow:0 0 0 1px var(--hair), 0 6px 22px rgba(28,29,29,.10); --a:33.333%; --b:66.667%; }
.cei__sheet canvas,.cei__sheet svg{ position:absolute; left:0; top:0; width:100%; height:100%; display:block; }
.cei__sheet[data-mode="split"] .cei__orig{ clip-path:inset(0 calc(100% - var(--a)) 0 0); }
.cei__sheet[data-mode="split"] .cei__grey{ clip-path:inset(0 calc(100% - var(--b)) 0 var(--a)); }
.cei__sheet[data-mode="split"] .cei__vec{ clip-path:inset(0 0 0 var(--b)); }
.cei__sheet[data-mode="orig"] .cei__grey,.cei__sheet[data-mode="orig"] .cei__vec,.cei__sheet[data-mode="grey"] .cei__vec,.cei__sheet[data-mode="grey"] .cei__orig,.cei__sheet[data-mode="vec"] .cei__orig,.cei__sheet[data-mode="vec"] .cei__grey{ visibility:hidden; }
.cei__sheet.is-erase{ cursor:crosshair; }
.cei__vec{ background:#fff; }
.cei__vec path{ fill-rule:evenodd; }
.cei__div{ position:absolute; top:0; bottom:0; width:14px; margin-left:-7px; cursor:ew-resize; z-index:2; touch-action:none; }
.cei__div::before{ content:""; position:absolute; left:6.5px; top:0; bottom:0; width:1px; background:var(--gold); box-shadow:0 0 0 1px rgba(255,255,255,.7); }
.cei__div::after{ content:""; position:absolute; left:3px; top:50%; width:8px; height:22px; margin-top:-11px; background:#fff; border:1px solid var(--gold); }
.cei__sheet:not([data-mode="split"]) .cei__div,.cei__sheet:not([data-mode="split"]) .cei__lab{ display:none; }
.cei__lab{ position:absolute; top:6px; transform:translateX(-50%); z-index:2; pointer-events:none; font-size:8.5px; font-weight:700; letter-spacing:.16em; text-transform:uppercase; color:var(--ink);
  background:rgba(255,255,255,.9); border:1px solid var(--hair); padding:2px 6px; white-space:nowrap; }
.cei__crop{ position:absolute; z-index:3; box-shadow:0 0 0 9999px rgba(28,29,29,.42); border:1px solid #fff; outline:1px dashed var(--ink); cursor:move; touch-action:none; }
.cei__crop i{ position:absolute; width:10px; height:10px; background:#fff; border:1px solid var(--ink); }
.cei__crop i[data-h="nw"]{ left:-6px; top:-6px; cursor:nwse-resize; } .cei__crop i[data-h="ne"]{ right:-6px; top:-6px; cursor:nesw-resize; }
.cei__crop i[data-h="sw"]{ left:-6px; bottom:-6px; cursor:nesw-resize; } .cei__crop i[data-h="se"]{ right:-6px; bottom:-6px; cursor:nwse-resize; }
.cei__wait{ position:absolute; right:8px; bottom:8px; z-index:3; font-size:9px; font-weight:700; letter-spacing:.14em; text-transform:uppercase; color:var(--muted); background:rgba(255,255,255,.92); padding:3px 7px; border:1px solid var(--hair); }
.cei__side{ grid-area:2/2/3/3; min-height:0; overflow-y:auto; border-left:1px solid var(--hair); padding:12px 16px 12px 14px; display:flex; flex-direction:column; gap:13px; }
.cei__sec > b{ display:block; font-size:9px; font-weight:700; letter-spacing:.18em; text-transform:uppercase; color:var(--muted); margin:0 0 7px; }
.cei__row{ display:flex; gap:3px; }
.cei__row .ce-act{ min-height:32px; }
.cei__row .ce-act.is-on,.cei__side .ce-act.is-on{ background:var(--ink); border-color:var(--ink); color:#fff; }
.cei__sec .ce-seg{ width:100%; margin:0 0 6px; } .cei__sec .ce-seg button{ flex:1 1 0; padding:0 6px; }
.cei__sl{ display:grid; grid-template-columns:78px minmax(0,1fr) 34px; align-items:center; gap:8px; min-height:26px; }
.cei__sl label{ font-size:11.5px; color:var(--ink); white-space:nowrap; }
.cei__sl output{ font-size:11px; font-weight:700; color:var(--ink); text-align:right; font-variant-numeric:tabular-nums; }
.cei__sl.is-off{ opacity:.38; }
.cei input[type=range]{ -webkit-appearance:none; appearance:none; width:100%; height:18px; margin:0; background:transparent; cursor:pointer; accent-color:var(--ink); }
.cei input[type=range]::-webkit-slider-runnable-track{ height:4px; background:#d8d4cd; border:1px solid #bdb8af; border-radius:999px; }
.cei input[type=range]::-webkit-slider-thumb{ -webkit-appearance:none; appearance:none; width:16px; height:16px; margin-top:-7px; border-radius:50%; border:2px solid #fff; background:var(--ink); box-shadow:0 0 0 1px var(--ink), 0 1px 3px rgba(0,0,0,.22); }
.cei input[type=range]::-moz-range-track{ height:4px; background:#d8d4cd; border:1px solid #bdb8af; border-radius:999px; }
.cei input[type=range]::-moz-range-progress{ height:4px; background:var(--gold); border-radius:999px; }
.cei input[type=range]::-moz-range-thumb{ width:14px; height:14px; border-radius:50%; border:2px solid #fff; background:var(--ink); box-shadow:0 0 0 1px var(--ink), 0 1px 3px rgba(0,0,0,.22); }
.cei input[type=range]:focus-visible{ outline:none; }
.cei input[type=range]:focus-visible::-webkit-slider-thumb{ box-shadow:0 0 0 1px var(--ink), 0 0 0 4px rgba(165,138,82,.23); }
.cei__tog{ position:relative; display:flex; align-items:center; gap:9px; width:100%; border:0; background:none; padding:5px 0; cursor:pointer; font-size:11.5px; color:var(--ink); text-align:left; }
.cei__tog .ce-tick{ flex:none; }
.cei__tog.is-on .ce-tick::after{ content:"✓"; position:absolute; inset:0; display:flex; align-items:center; justify-content:center; font-size:11px; line-height:1; }
.cei__more{ border:0; background:none; padding:4px 0; cursor:pointer; font-size:10px; font-weight:700; letter-spacing:.14em; text-transform:uppercase; color:var(--muted); }
.cei__more:hover{ color:var(--ink); }
.cei__sug{ display:flex; align-items:center; gap:8px; font-size:11px; line-height:1.4; color:var(--muted); margin:2px 0 4px; min-height:26px; }
.cei__sug b{ color:var(--ink); }
.cei__sug .ce-prop{ height:24px; margin-left:auto; flex:none; }
.cei__note{ font-size:11px; line-height:1.55; color:var(--muted); margin:0; }
.cei__note b{ color:var(--ink); }
.cei__greys{ display:flex; gap:3px; margin-top:6px; } .cei__greys i{ flex:1; height:10px; border:1px solid var(--hair); }
.cei__foot{ grid-area:3/1/4/3; display:flex; align-items:center; gap:14px; padding:11px 18px 12px; border-top:1px solid var(--hair); min-width:0; }
.cei__into{ display:flex; align-items:center; gap:9px; min-width:0; flex:1 1 auto; font-size:11px; color:var(--muted); }
.cei__into .ce-seg{ flex:none; }
.cei__into span{ white-space:nowrap; overflow:hidden; text-overflow:ellipsis; min-width:0; }
.cei__prog{ flex:1 1 auto; min-width:0; display:flex; align-items:center; gap:10px; }
.cei__prog span{ font-size:11px; font-weight:700; letter-spacing:.06em; color:var(--ink); white-space:nowrap; }
.cei__bar{ flex:1 1 auto; height:3px; background:#e2ded7; min-width:40px; } .cei__bar i{ display:block; height:100%; width:0; background:var(--gold); transition:width .2s ease; }
.cei__foot .ce-actions{ margin-left:auto; }
@media (max-width:760px){
  .cei{ padding:0; }
  .cei__card{ width:100%; height:100%; grid-template-columns:minmax(0,1fr); grid-template-rows:auto minmax(0,1fr) auto auto; border:0; }
  .cei__head{ grid-area:1/1/2/2; padding:10px 12px 8px; } .cei__head h3{ font-size:19px; }
  .cei__view{ grid-area:2/1/3/2; padding:8px 10px; } .cei__hint{ display:none; }
  .cei__side{ grid-area:3/1/4/2; border-left:0; border-top:1px solid var(--hair); max-height:42vh; padding:10px 12px; }
  .cei__foot{ grid-area:4/1/5/2; padding:8px 12px; flex-wrap:wrap; gap:8px; } .cei__into span{ display:none; }
}
@media (prefers-reduced-motion: reduce){ .cei__card{ animation:none; } .cei__bar i{ transition:none; } }
`;
  function injectCss() { if (!doc || doc.getElementById('ceiCss')) return; const s = doc.createElement('style'); s.id = 'ceiCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s); }

  /* the one Image step on screen (null when none), and what the tests read: the main-thread milliseconds of each change, made and drawn */
  let live = null;
  const stats = { inputMs: [], byKind: {}, previewTraces: 0, placeMs: 0, lastTrace: null };
  const took = (kind, ms) => { stats.inputMs.push(ms); if (stats.inputMs.length > 400) stats.inputMs.shift(); (stats.byKind[kind] || (stats.byKind[kind] = [])).push(ms); };

  /** THE IMAGE STEP over the editor. info.source: the decoded picture; o: the settings to start from; info.retrace / info.box / info.src: Re-trace's (the box the set stands in now).
   *  → Promise<the group made | null> */
  function step(ctx, o, info) {
    info = info || {};
    injectCss();
    const host = (ctx.root && ctx.root.querySelector('.ce-win')) || ctx.root || doc.body;
    const T = tracer();
    let src = info.source || null, st = norm(o), mode = 'split', tool = null, closed = false, placing = false, prev = null, tWant = 0, tRun = null, split = [1 / 3, 2 / 3], more = false, tol = 18;
    let useSel = !info.retrace && ctx.selection().length > 0;
    const box = doc.createElement('div'); box.className = 'cei';
    const sl = (k, label, min, max, stepv, val, show, tip) => `<div class="cei__sl" data-sl="${k}"${tip ? ` title="${esc(tip)}"` : ''}><label for="cei-${k}">${label}</label><input id="cei-${k}" type="range" data-k="${k}" min="${min}" max="${max}" step="${stepv}" value="${val}"><output data-o="${k}">${show}</output></div>`;
    const tog = (k, label, tip) => `<button class="cei__tog" type="button" data-tog="${k}" aria-pressed="false"${tip ? ` title="${esc(tip)}"` : ''}><i class="ce-tick"></i>${label}</button>`;
    box.innerHTML = `
<div class="cei__card" role="dialog" aria-modal="true" aria-label="Image" tabindex="-1">
  <header class="cei__head"><h3>${info.retrace ? 'Re-trace image' : 'Image'}</h3><span class="cei__eyebrow" data-name></span></header>
  <div class="cei__view">
    <div class="cei__modes">
      <div class="ce-seg" role="tablist" aria-label="Preview">
        <button type="button" data-mode="split" role="tab">Split</button><button type="button" data-mode="orig" role="tab">Original</button><button type="button" data-mode="grey" role="tab">Greyscale</button><button type="button" data-mode="vec" role="tab">Vector</button>
      </div>
      <span class="cei__hint" data-hint></span>
      <div class="cei__tol" data-tolrow hidden><label for="cei-tol">How close a shade counts</label><input id="cei-tol" type="range" data-tol min="2" max="70" step="1" value="18"><output data-tolv>18</output><button class="ce-prop" type="button" data-do="restore" disabled>Restore</button></div>
    </div>
    <div class="cei__room" data-room>
      <div class="cei__sheet" data-mode="split">
        <canvas class="cei__orig" aria-label="The picture"></canvas><canvas class="cei__grey" aria-label="The picture in grey"></canvas>
        <svg class="cei__vec" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none" aria-label="The picture as vectors"></svg>
        <span class="cei__lab" data-lab="0">Original</span><span class="cei__lab" data-lab="1">Greyscale</span><span class="cei__lab" data-lab="2">Vector</span>
        <i class="cei__div" data-div="0" role="separator" aria-label="Original | greyscale"></i><i class="cei__div" data-div="1" role="separator" aria-label="Greyscale | vector"></i>
        <div class="cei__crop" data-crop hidden><i data-h="nw"></i><i data-h="ne"></i><i data-h="sw"></i><i data-h="se"></i></div>
      </div>
      <span class="cei__wait" data-wait hidden>Tracing…</span>
    </div>
  </div>
  <div class="cei__side">
    <div class="cei__sec"><b>Picture</b>
      <div class="cei__row">
        <button class="ce-act" type="button" data-do="left" title="Turn 90° left" aria-label="Turn 90° left">${IC.left}</button><button class="ce-act" type="button" data-do="right" title="Turn 90° right" aria-label="Turn 90° right">${IC.right}</button>
        <button class="ce-act" type="button" data-do="flipH" title="Flip left to right" aria-label="Flip left to right">${IC.flipH}</button><button class="ce-act" type="button" data-do="flipV" title="Flip top to bottom" aria-label="Flip top to bottom">${IC.flipV}</button>
        <button class="ce-act" type="button" data-do="crop" title="Crop: drag the box's corners, or inside it to move it" aria-label="Crop" aria-pressed="false">${IC.crop}</button>
        <button class="ce-act" type="button" data-do="erase" title="Remove the background: click it and the area round the click turns white (white is never engraved)" aria-label="Remove the background" aria-pressed="false">${IC.erase}</button>
      </div>
    </div>
    <div class="cei__sec"><b>Tone</b>
      ${sl('brightness', 'Brightness', -100, 100, 1, 0, 0)}
      ${sl('contrast', 'Contrast', -100, 100, 1, 0, 0)}
      ${tog('invert', 'Invert: the light parts engraved')}
      <button class="cei__more" type="button" data-do="more" aria-expanded="false">More ▸</button>
      <div data-moreblock hidden>
        ${tog('auto', 'Auto levels', 'The darkest part made black and the lightest white (half a percent clipped at each end)')}
        ${sl('gamma', 'Gamma', 30, 300, 1, 100, '1.00', 'The middle greys lighter (more than 1) or darker')}
        ${sl('sharpen', 'Sharpen', 0, 100, 1, 0, 0, 'Unsharp mask: edges crisper before the trace')}
        ${tog('denoise', 'Denoise (median)', 'Specks and grain taken out before the trace')}
      </div>
    </div>
    <div class="cei__sec"><b>Trace</b>
      <div class="ce-seg" role="tablist" aria-label="Trace as"><button type="button" data-tmode="grey" role="tab">Greys</button><button type="button" data-tmode="line" role="tab" title="For photos of handwriting and signatures, and line art lit unevenly: ink wherever the picture is darker than around it">Line art</button></div>
      ${sl('levels', 'Levels', 1, 8, 1, st.levels, st.levels)}
      <div class="cei__sug" data-sug hidden></div>
      ${sl('detail', 'Detail', 0, 100, 1, 50, 50)}
      ${sl('smooth', 'Smoothness', 0, 100, 1, 50, 50)}
      ${sl('despeckle', 'Despeckle', 0, 10, 1, 2, 2)}
      <div class="cei__greys" data-greys aria-hidden="true"></div>
    </div>
    <p class="cei__note" data-note></p>
  </div>
  <footer class="cei__foot">
    <div class="cei__into" data-into><div class="ce-seg" role="tablist" aria-label="Fit into"><button type="button" data-into="outline" role="tab">Charm</button><button type="button" data-into="selection" role="tab">Selection</button></div><span data-where></span></div>
    <div class="cei__prog" data-prog hidden><span data-pct>Tracing 0%</span><div class="cei__bar"><i data-barfill></i></div></div>
    <div class="ce-actions"><button class="ce-btn ce-btn--ghost" type="button" data-do="cancel">Cancel</button><button class="ce-btn ce-btn--gold" type="button" data-do="place">${info.retrace ? 'Re-trace' : 'Place on charm'}</button></div>
  </footer>
</div>`;
    host.appendChild(box);
    const $ = s => box.querySelector(s), $$ = s => Array.from(box.querySelectorAll(s));
    const card = $('.cei__card'), sheet = $('.cei__sheet'), room = $('[data-room]'), cvO = $('.cei__orig'), cvG = $('.cei__grey'), svg = $('.cei__vec'), cropEl = $('[data-crop]');
    // (the editor reads every key on the window first and hands it to an element's own __ceKey: everything in here takes its keys, so Delete, the arrows, ⌘Z never reach the charm
    // behind the card; a slider still moves with its arrows, a button still presses with Enter)
    const onKey = e => {
      const k = e.key;
      if (k === 'Escape') { e.preventDefault(); if (tool) setTool(null); else if (placing) cancelPlace(); else finish(null); return; }
      if (k === 'Enter' && !(e.target && e.target.tagName === 'BUTTON')) { e.preventDefault(); place(); return; }
      if ((e.metaKey || e.ctrlKey) && !/^[vcx]$/i.test(k)) e.preventDefault();
    };
    const tagKeys = el => { el.__ceKey = onKey; };
    tagKeys(box); tagKeys(card); $$('button,input').forEach(tagKeys);
    box.addEventListener('focusin', e => { if (e.target && !e.target.__ceKey) tagKeys(e.target); });
    // a file dropped on the card replaces the picture in it (the card is over the work area, so the editor's own drop never sees it)
    box.addEventListener('dragover', e => { if (e.dataTransfer && Array.from(e.dataTransfer.types || []).includes('Files')) { e.preventDefault(); e.dataTransfer.dropEffect = 'copy'; } });
    box.addEventListener('drop', e => { e.preventDefault(); e.stopPropagation(); const f = Array.from((e.dataTransfer && e.dataTransfer.files) || []).find(accepts); if (f) swap(f, 'drop'); });

    const nameIt = () => { $('[data-name]').textContent = src ? `${src.name} · ${src.w0} × ${src.h0} px` : ''; };
    const whole = () => tool === 'crop' || tool === 'erase';   // (cropping or erasing: the whole picture shown, the crop not applied)
    /* the preview: the original and the grey drawn here at once (the filters kept while only the tone changes), the vector asked of the worker */
    let gImg = null, pxCache = { src: null }, shownPx = null, lastPrep = null;
    function layout() {
      if (!src) return;
      const [kw, kh] = whole() ? shownSize(src.w, src.h, st) : keptSize(src.w, src.h, st), r = room.getBoundingClientRect(), pad = 14;
      const k = Math.min((r.width - 2 * pad) / kw, (r.height - 2 * pad) / kh);
      if (!(k > 0)) return;
      sheet.style.width = Math.max(10, Math.round(kw * k)) + 'px'; sheet.style.height = Math.max(10, Math.round(kh * k)) + 'px';
      placeLabels(); if (tool === 'crop') drawCropBox();
    }
    /** The picture as shown drawn again (turned, flipped, cropped, erased), then its grey. */
    function paint(kind) {
      if (!src) return;
      const t0 = now();
      const [kw, kh] = whole() ? shownSize(src.w, src.h, st) : keptSize(src.w, src.h, st), [w, h] = fitPx(kw, kh, SHOW_MAX);
      if (cvO.width !== w || cvO.height !== h) { cvO.width = cvG.width = w; cvO.height = cvG.height = h; }
      drawShown(cvO, src, st, whole());
      shownPx = cvO.getContext('2d').getImageData(0, 0, w, h).data;
      greyNow();
      layout(); where();
      took(kind || 'picture', now() - t0);
    }
    /** The grey made again from the picture as shown (only the lookup table when only the tone changed). A big picture's median and sharpen are made in the worker (they are
     *  5 ms on a quiet computer and 50 on a busy one): the grey is drawn when they are back, and traced then. */
    let baseSeq = 0;
    function greyNow(kind) {
      if (!src || !shownPx) return;
      const t0 = now(), w = cvO.width, h = cvO.height, gx = cvG.getContext('2d'), key = filterKey(w, h, st), mine = ++baseSeq;
      if ((st.denoise || st.sharpen > 0) && w * h > 20000 && !(pxCache.src === shownPx && pxCache.key === key)) {
        const px = shownPx, p = baseAsync(px, w, h, st);
        if (p) {
          p.then(base => { if (closed || mine !== baseSeq || px !== shownPx) return; pxCache = { src: px, key, base, luma: null }; greyNow(); traceSoon(); }, () => {});
          if (kind) took(kind, now() - t0);
          return;
        }
      }
      lastPrep = prepGrey(shownPx, w, h, st, pxCache);
      if (!gImg || gImg.width !== w || gImg.height !== h) gImg = gx.createImageData(w, h);
      const d = gImg.data, g = lastPrep.g;
      for (let i = 0, p = 0; i < g.length; i++, p += 4) { d[p] = d[p + 1] = d[p + 2] = g[i]; d[p + 3] = 255; }
      gx.putImageData(gImg, 0, 0);
      suggest(!kind || kind === 'picture' || kind === 'mode');
      if (kind) took(kind, now() - t0);
    }
    function traceSoon() {
      tWant++;
      if (!src || whole()) return;
      if (!T) { const wt = $('[data-wait]'); wt.hidden = false; wt.textContent = 'Tracing is not on this page yet'; return; }
      if (!tRun) runPreview();
    }
    /** The vector preview: the grey as shown, at most 280 px, traced in the worker; when it is back the newest settings are traced (never a queue). */
    function runPreview() {
      const v = tWant, [w, h] = fitPx(cvG.width, cvG.height, PREVIEW_MAX), c = canvas(w, h), g = c.getContext('2d', RD);
      g.imageSmoothingQuality = 'high'; g.drawImage(cvG, 0, 0, w, h);
      const input = g.getImageData(0, 0, w, h), o = Object.assign({}, st), wt = $('[data-wait]');
      wt.hidden = false; wt.textContent = 'Tracing…';
      const t0 = now();
      const mm = tgt ? Math.max(tgt.box[2] - tgt.box[0], tgt.box[3] - tgt.box[1]) * MM : 0;
      tRun = Promise.resolve().then(() => T.trace(input, traceOpts(o, mm > 0 ? Math.max(w, h) / mm : 20)))
        .then(res => { if (closed || !res) return; stats.previewTraces++; stats.lastTrace = { ms: Math.round(now() - t0), w: res.width, h: res.height }; prev = res; drawVector(res); })
        .catch(e => { if (!closed && !(e && (e.cancelled || e.name === 'AbortError'))) console.warn('image: preview trace', e); })
        .then(() => { tRun = null; if (closed) return; if (tWant !== v && !whole()) runPreview(); else wt.hidden = true; });
    }
    function drawVector(res) {
      svg.setAttribute('viewBox', `0 0 ${res.width} ${res.height}`);
      const layers = (res.layers || []).filter(L => L && L.subpaths && L.subpaths.length && +L.grey < 0.999).slice().sort((a, b) => b.grey - a.grey);
      svg.innerHTML = layers.map(L => `<path d="${dOf(L.subpaths)}" fill="${rgb(+L.grey)}"/>`).join('');
      $('[data-greys]').innerHTML = layers.slice().reverse().map(L => `<i style="background:${rgb(+L.grey)}" title="${esc(greyName(+L.grey))}"></i>`).join('');
      note(sizeOf(layers));
    }
    /** What the trace makes of a picture, said plainly (the lead, 28 Oct 2026: "say plainly in the panel that a photo becomes a posterised (stencil-like) engraving in N greys"). */
    function note(s) {
      const n = levelsOf(st), pts = s ? ` <b>${s.ops.toLocaleString('en')}</b> points in the preview.` : '';
      $('[data-note]').innerHTML = st.mode === 'line'
        ? `<b>Line art:</b> black only, inked wherever the picture is darker than the paper round it, for handwriting and signatures photographed in uneven light.${pts}`
        : n === 1 ? `<b>Black only:</b> everything darker than the middle grey is engraved, the rest is left.${pts}`
          : `A photo becomes a <b>posterised, stencil-like engraving in ${n} greys</b>, not a photograph: black engraves deepest, white is not engraved, and each grey is a laser layer of its own.${pts}`;
    }
    /** The level count this picture asks for, offered beside the slider: the trace's own suggestion (CharmNestTrace.suggest) when it makes one, else this file's; worked out once for
     *  each picture as shown (fresh: a new picture, a turn, a crop, the background taken away), then only said again. */
    let sug = null;
    function suggest(fresh) {
      if (fresh || !sug) {
        let s = null;
        try { if (T && typeof T.suggest === 'function' && gImg) { const r = T.suggest(gImg); const lv = r && typeof r === 'object' ? +r.levels : +r; if (lv >= 1 && lv <= 8) s = { levels: Math.round(lv), kind: KIND[r && r.content] || (r && r.kind) || 'this picture' }; } } catch (e) { s = null; }
        sug = s || suggestLevels(lastPrep);
      }
      const el = $('[data-sug]');
      if (!sug || st.mode === 'line') { el.hidden = true; return; }
      el.hidden = false;
      el.innerHTML = `<span>Looks like ${esc(sug.kind)}: <b>${sug.levels === 1 ? 'black only' : sug.levels + ' greys'}</b> suggested</span>`
        + (sug.levels !== st.levels ? `<button class="ce-prop" type="button" data-do="usesug">Use ${sug.levels}</button>` : '');
      const b = el.querySelector('button'); if (b) tagKeys(b);
    }
    /* where it will go, said under the preview */
    let tgt = null, tgtKey = '';
    function where() {
      if (!src) return;
      const [kw, kh] = keptSize(src.w, src.h, st);
      const has = ctx.selection().length > 0;
      if (info.retrace) $('[data-into] .ce-seg').hidden = true;
      else { $('[data-into="selection"]').disabled = !has; if (!has) useSel = false; $$('button[data-into]').forEach(x => x.classList.toggle('is-on', (x.dataset.into === 'selection') === useSel)); }
      const key = (kw / kh).toFixed(5) + '|' + useSel;   // (the charm does not change while the step is open: the box is worked out once for each shape of picture)
      if (!tgt || tgtKey !== key) { tgtKey = key; tgt = info.retrace ? { box: fitInBox(kw / kh, info.box), what: 'where the image is now', into: 'retrace' } : target(ctx, kw / kh, useSel); }
      const b = tgt.box;
      $('[data-where]').textContent = `${((b[2] - b[0]) * MM).toFixed(2)} × ${((b[3] - b[1]) * MM).toFixed(2)} mm, ${tgt.into === 'retrace' ? tgt.what : 'centred in ' + tgt.what}`;
    }
    function placeLabels() {
      sheet.style.setProperty('--a', (split[0] * 100) + '%'); sheet.style.setProperty('--b', (split[1] * 100) + '%');
      $$('.cei__div').forEach((d, i) => { d.style.left = (split[i] * 100) + '%'; });
      const mids = [split[0] / 2, (split[0] + split[1]) / 2, (split[1] + 1) / 2];
      $$('.cei__lab').forEach((l, i) => { l.style.left = (mids[i] * 100) + '%'; l.hidden = (i === 0 ? split[0] : i === 1 ? split[1] - split[0] : 1 - split[1]) < 0.12; });
    }
    function setMode(m) {
      mode = m; sheet.dataset.mode = whole() ? 'orig' : m;
      $$('button[data-mode]').forEach(b => b.classList.toggle('is-on', b.dataset.mode === m));
      $('[data-hint]').textContent = tool === 'crop' ? 'Drag the corners · Esc or Crop again when done' : m === 'split' ? 'Drag the gold lines to compare'
        : m === 'vec' ? 'What the laser engraves' : m === 'grey' ? 'Black is the base: it engraves deepest' : 'As it came in';
      $('[data-hint]').hidden = tool === 'erase'; $('[data-tolrow]').hidden = tool !== 'erase';
    }
    /** Crop or erase in hand (null: neither): the whole picture is shown while one is. */
    function setTool(t) {
      const was = tool; tool = t === was ? null : t;
      for (const k of ['crop', 'erase']) { const b = $(`[data-do="${k}"]`); b.classList.toggle('is-on', tool === k); b.setAttribute('aria-pressed', String(tool === k)); }
      cropEl.hidden = tool !== 'crop'; sheet.classList.toggle('is-erase', tool === 'erase');
      setMode(mode); paint('tool');
      if (!tool) traceSoon();
    }
    function drawCropBox() {
      const c = st.crop || [0, 0, 1, 1];
      Object.assign(cropEl.style, { left: c[0] * 100 + '%', top: c[1] * 100 + '%', width: (c[2] - c[0]) * 100 + '%', height: (c[3] - c[1]) * 100 + '%' });
    }
    let drag = null;
    cropEl.addEventListener('pointerdown', e => {
      e.preventDefault(); e.stopPropagation(); const r = sheet.getBoundingClientRect();
      drag = { kind: 'crop', h: e.target.dataset.h || 'move', x: e.clientX, y: e.clientY, w: r.width, hgt: r.height, c: (st.crop || [0, 0, 1, 1]).slice() };
      cropEl.setPointerCapture(e.pointerId);
    });
    cropEl.addEventListener('pointermove', e => {
      if (!drag || drag.kind !== 'crop') return;
      const dx = (e.clientX - drag.x) / drag.w, dy = (e.clientY - drag.y) / drag.hgt, c = drag.c.slice(), MIN = 0.04;
      if (drag.h === 'move') { const w = c[2] - c[0], h = c[3] - c[1]; c[0] = clamp(c[0] + dx, 0, 1 - w); c[1] = clamp(c[1] + dy, 0, 1 - h); c[2] = c[0] + w; c[3] = c[1] + h; }
      else {
        if (drag.h.includes('w')) c[0] = clamp(c[0] + dx, 0, c[2] - MIN); if (drag.h.includes('e')) c[2] = clamp(c[2] + dx, c[0] + MIN, 1);
        if (drag.h.includes('n')) c[1] = clamp(c[1] + dy, 0, c[3] - MIN); if (drag.h.includes('s')) c[3] = clamp(c[3] + dy, c[1] + MIN, 1);
      }
      st = norm(Object.assign({}, st, { crop: c })); drawCropBox();
    });
    const endCrop = () => { if (drag && drag.kind === 'crop') { drag = null; where(); } };
    cropEl.addEventListener('pointerup', endCrop); cropEl.addEventListener('pointercancel', endCrop);
    // the background: a click on the picture (the whole of it is shown) floods the area round it to white
    sheet.addEventListener('pointerdown', e => {
      if (tool !== 'erase' || e.button) return;
      e.preventDefault();
      const t0 = now(), r = sheet.getBoundingClientRect(), [sw, sh] = shownSize(src.w, src.h, st);
      const q = inv(shownMatrix(src.w, src.h, st)), x = (e.clientX - r.left) / r.width * sw, y = (e.clientY - r.top) / r.height * sh;
      const n = eraseAt(src, q[0] * x + q[2] * y + q[4], q[1] * x + q[3] * y + q[5], tol);
      $('[data-do="restore"]').disabled = !src.orig;
      if (n) { log('imageErase', { name: src.name, px: n, tol }); paint('erase'); }
      took('eraseClick', now() - t0);
    });
    /* the two dividers of the split */
    $$('.cei__div').forEach((d, i) => {
      d.addEventListener('pointerdown', e => { e.preventDefault(); drag = { kind: 'div', i }; d.setPointerCapture(e.pointerId); });
      d.addEventListener('pointermove', e => {
        if (!drag || drag.kind !== 'div' || drag.i !== i) return;
        const r = sheet.getBoundingClientRect(), x = clamp((e.clientX - r.left) / r.width, 0, 1);
        split[i] = i === 0 ? Math.min(x, split[1]) : Math.max(x, split[0]); placeLabels();
      });
      const end = () => { drag = null; }; d.addEventListener('pointerup', end); d.addEventListener('pointercancel', end);
    });
    /* the controls */
    const TONE = new Set(['brightness', 'contrast', 'gamma']);
    box.addEventListener('input', e => {
      const el = e.target;
      if (el && el.dataset && el.dataset.tol != null) { tol = +el.value; $('[data-tolv]').textContent = String(tol); return; }
      const k = el && el.dataset && el.dataset.k; if (!k) return;
      const v = +el.value, o2 = Object.assign({}, st);
      o2[k] = k === 'levels' || k === 'despeckle' ? v : v / 100;
      st = norm(o2);
      $(`[data-o="${k}"]`).textContent = k === 'gamma' ? (v / 100).toFixed(2) : String(Math.round(v));
      if (TONE.has(k) || k === 'sharpen') greyNow(k);
      if (k === 'levels') { note(null); suggest(); }
      traceSoon();
    });
    box.addEventListener('click', e => {
      const t = e.target.closest('button'); if (!t || t.disabled) return;
      if (t.dataset.mode) { if (tool) setTool(null); setMode(t.dataset.mode); return; }
      if (t.dataset.into) { useSel = t.dataset.into === 'selection'; where(); return; }
      if (t.dataset.tmode) { st = norm(Object.assign({}, st, { mode: t.dataset.tmode })); sync(); greyNow('mode'); note(null); traceSoon(); return; }
      if (t.dataset.tog) { const k = t.dataset.tog; st = norm(Object.assign({}, st, { [k]: k === 'auto' ? !st.auto : !st[k] })); sync(); greyNow(k); traceSoon(); return; }
      const a = t.dataset.do; if (!a) return;
      if (a === 'cancel') { if (placing) cancelPlace(); else finish(null); }
      else if (a === 'place') place();
      else if (a === 'left' || a === 'right') { st = turn(st, a === 'left' ? -1 : 1); paint('turn'); traceSoon(); }
      else if (a === 'flipH' || a === 'flipV') { st = flip(st, a === 'flipH'); paint('flip'); traceSoon(); }
      else if (a === 'crop' || a === 'erase') setTool(a);
      else if (a === 'restore') { if (restoreSource(src)) { t.disabled = true; paint('restore'); } }
      else if (a === 'more') { more = !more; $('[data-moreblock]').hidden = !more; t.setAttribute('aria-expanded', String(more)); t.textContent = more ? 'Less ▾' : 'More ▸'; }
      else if (a === 'usesug' && sug) { st = norm(Object.assign({}, st, { levels: sug.levels, mode: 'grey' })); sync(); suggest(); note(null); traceSoon(); }
    });
    /** The controls say the settings (after a Use, a mode, Re-trace's own). */
    function sync() {
      const val = { brightness: st.brightness * 100, contrast: st.contrast * 100, gamma: st.gamma * 100, sharpen: st.sharpen * 100, levels: st.levels, detail: st.detail * 100, smooth: st.smooth * 100, despeckle: st.despeckle };
      for (const k of Object.keys(val)) { const inp = $(`[data-k="${k}"]`), out = $(`[data-o="${k}"]`); if (inp) inp.value = String(Math.round(val[k])); if (out) out.textContent = k === 'gamma' ? st.gamma.toFixed(2) : String(Math.round(val[k])); }
      for (const k of ['invert', 'auto', 'denoise']) { const b = $(`[data-tog="${k}"]`), on = !!st[k]; b.classList.toggle('is-on', on); b.setAttribute('aria-pressed', String(on)); }
      $$('button[data-tmode]').forEach(b => b.classList.toggle('is-on', b.dataset.tmode === st.mode));
      const lv = $('[data-sl="levels"]'); lv.classList.toggle('is-off', st.mode === 'line'); $('[data-k="levels"]').disabled = st.mode === 'line';
      if (st.mode === 'line') $('[data-o="levels"]').textContent = '1';
    }
    const ro = root.ResizeObserver ? new root.ResizeObserver(() => layout()) : null; if (ro) ro.observe(room);

    /* PLACE: the full trace in the worker, with progress; too big for a piece's record, traced again smaller */
    let resolveStep, placeToken = 0;
    const done = new Promise(r => { resolveStep = r; });
    function cancelPlace() { placeToken++; placing = false; try { if (T && T.cancel) T.cancel(); } catch (e) { /* fine */ } progress(null); }
    function progress(p, text) {
      $('[data-prog]').hidden = p == null; $('[data-into]').hidden = p != null; $('[data-do="place"]').disabled = p != null;
      if (p != null) { $('[data-barfill]').style.width = Math.round(clamp(p, 0, 1) * 100) + '%'; $('[data-pct]').textContent = text || `Tracing ${Math.round(clamp(p, 0, 1) * 100)}%`; }
    }
    async function place() {
      if (placing || !src) return;
      if (!T) { $('[data-note]').innerHTML = '<b>Tracing is not on this page yet:</b> reload the page and import again.'; return; }
      if (tool) setTool(null);
      placing = true; const token = ++placeToken, t0 = now();
      where();
      const box0 = tgt.box, o2 = Object.assign({}, st), [kw, kh] = keptSize(src.w, src.h, o2);
      // (never enlarged: the trace measured that an enlarged picture makes no truer outline and three or four times the points, 28 Oct 2026)
      const longMm = Math.max(box0[2] - box0[0], box0[3] - box0[1]) * MM;
      let px = Math.min(clamp(Math.round(longMm * FULL_PX_MM), 16, FULL_MAX), Math.round(Math.max(kw, kh)));
      let res = null, size = null, tries = 0, smaller = false;
      progress(0);
      try {
        for (;;) {
          const input = await greyPixels(src, o2, px);
          if (token !== placeToken || closed) return;
          res = await T.trace(input, Object.assign(traceOpts(o2, Math.max(input.width, input.height) / longMm), { onProgress: p => { if (token === placeToken) progress(tries ? 0.5 + p * 0.5 : p); } }));
          if (token !== placeToken || closed) return;
          size = sizeOf(res.layers);
          if (fits(size) || ++tries > 4 || px <= 64) break;
          smaller = true; px = Math.max(64, Math.round(px * 0.72));
          progress(0.5, 'Simplifying to fit what one piece can hold…');
        }
      } catch (e) {
        if (token !== placeToken || closed) return;
        placing = false; progress(null); $('[data-note]').innerHTML = `<b>The picture was not traced:</b> ${esc(String((e && e.message) || e).slice(0, 160))}`; return;
      }
      if (!fits(size)) { placing = false; progress(null); $('[data-note]').innerHTML = `<b>Too detailed for one charm</b> (${size.ops.toLocaleString('en')} points): lower Detail, raise Despeckle or use fewer levels.`; return; }
      // (the group's id is its source's: a piece saved and opened again gives its parts back with it as their `group`, and Re-trace finds the picture by it)
      const srcId = info.src || newId();
      const g = buildGroup(res, box0, { id: info.groupId || srcId, src: srcId, settings: o2, name: src.name });
      if (!g.children.length) { placing = false; progress(null); $('[data-note]').innerHTML = '<b>Nothing to engrave:</b> the picture traced as white. Lower the brightness, or turn Invert on.'; return; }
      keepSource(srcId, Object.assign(src, { settings: o2 }));
      stats.placeMs = Math.round(now() - t0);
      placing = false;
      log(info.retrace ? 'imageRetrace' : 'imageTrace', { name: src.name, mode: o2.mode, levels: levelsOf(o2), layers: g.children.length, ops: size.ops, px: res.width + 'x' + res.height,
        mm: [+((box0[2] - box0[0]) * MM).toFixed(2), +((box0[3] - box0[1]) * MM).toFixed(2)], into: tgt.into, ms: stats.placeMs, simplified: smaller });
      finish(g, smaller);
    }
    function finish(g, smaller) {
      if (closed) return; closed = true; placeToken++;
      if (ro) ro.disconnect();
      box.remove(); live = null;
      try { const w = ctx.root && ctx.root.querySelector('.ce-win'); if (w) w.focus({ preventScroll: true }); } catch (e) { /* fine */ }
      if (g && smaller && ctx.say) ctx.say('Placed · simplified to fit what one piece can hold', 4000);
      resolveStep(g || null);
    }
    async function swap(file, via) {
      try {
        const s = await decode(file); if (closed) return;
        src = s; info.src = null; pxCache = { src: null }; $('[data-do="restore"]').disabled = true;
        log('imageImport', { name: s.name, type: s.type, px: s.w0 + 'x' + s.h0, via });
        nameIt(); st = norm(Object.assign({}, st, { crop: null })); paint('swap'); traceSoon();
      } catch (e) { $('[data-note]').innerHTML = `<b>${esc(file.name || 'That file')}</b> could not be read: ${esc((e && e.message) || e)}`; }
    }
    live = { swap, place: () => place(), cancel: () => finish(null), settings: () => Object.assign({}, st), target: () => tgt && Object.assign({}, tgt, { box: tgt.box.slice() }),
      preview: () => prev, suggestion: () => sug, busy: () => !!tRun || placing, done };
    nameIt(); sync(); setMode('split'); note(null);
    // (the card is laid out by the browser in this frame; the picture is drawn in the next, so neither is one long task)
    requestAnimationFrame(() => setTimeout(() => { if (closed) return; paint('open'); traceSoon(); try { card.focus({ preventScroll: true }); } catch (e) { /* fine */ } }, 0));
    return done;
  }
  function accepts(f) {
    const n = String((f && f.name) || '').toLowerCase(), t = String((f && f.type) || '').toLowerCase();
    return ACCEPT.some(a => (a[0] === '.' ? n.endsWith(a) : t === a));
  }

  /** The importer: a picture file → the Image step → one group on the charm. */
  async function run(ctx, file, info) {
    info = info || {};
    if (live) { live.swap(file, info.via || 'button'); return null; }   // (a second picture while the step is open takes the first's place)
    let src;
    try { src = await decode(file); }
    catch (e) { if (ctx.say) ctx.say(`${file && file.name || 'That file'} could not be read: ${String((e && e.message) || e).slice(0, 100)}`, 5000); return null; }
    log('imageImport', { name: src.name, type: src.type, px: src.w0 + 'x' + src.h0, via: info.via || 'button' });
    if (ctx.say) ctx.say('');
    const g = await step(ctx, null, { source: src });
    if (!g) return null;
    ctx.addItems([g], { select: true });
    ctx.select([g.id]);
    ctx.commit('Place image');
    return g;
  }
  /** RE-TRACE: the selected traced image made again from its source with new settings, where it is now (moved, scaled and turned as it is). */
  async function retrace(ctx) {
    const set = traceSet(ctx); if (!set) return null;
    const src = SOURCES.get(set.src);
    if (!src) { if (ctx.say) ctx.say('The picture this was traced from is not on this page any more: import it again to trace it afresh', 5000); return null; }
    const M = ctx.model, geo = ctx.geo;
    // the map from where it was placed to where it is now: the group's own matrix (with its parents'), or its first part's (a group's transform is pushed into its parts on every edit)
    let W = M.I, box = set.box;
    if (set.kind === 'group') {
      const e = ctx.entry(set.id), lf = (set.it.children || []).find(c => c.kind === 'path');
      const lfE = lf && ctx.entry(lf.id);
      W = lfE ? M.mul(lfE.P, geo.matrix(lf)) : M.mul(e.P, geo.matrix(set.it));
    }
    if (!box) {   // (a piece opened again: its parts are where they were drawn, flat; the trace goes into their box)
      let b = null; for (const id of set.ids || []) { const e = ctx.entry(id); const d = e && geo.drawn(e.it, e.P); if (d) b = b ? [Math.min(b[0], d[0]), Math.min(b[1], d[1]), Math.max(b[2], d[2]), Math.max(b[3], d[3])] : d.slice(); }
      box = b; W = M.I;
    }
    if (!box) return null;
    const g = await step(ctx, set.settings || src.settings || null, { source: src, retrace: true, box, src: set.src, groupId: set.kind === 'group' ? set.id : set.src });
    if (!g) return null;
    if (set.kind === 'group') { g.name = set.it.name || g.name; g.trace.box = box.map(r4); }
    // the new parts put where the old ones are: the same map on each of them (CharmEditor.model.composeInto: a turn and two scales, exactly as the editor keeps a moved part)
    if (!M.isI(W)) g.children = g.children.map(ch => M.composeInto(ch, W, geo));
    const map = {};
    if (set.kind === 'group') map[set.id] = g;
    else { set.ids.forEach((id, i) => { map[id] = i === 0 ? g : null; }); }
    ctx.replaceItems(map);
    ctx.select([g.id]);
    ctx.commit('Re-trace image');
    return g;
  }

  /** Registers the importer and the Re-trace command with the Charm Editor (once; before or after charm-nest-charm-editor.js loads). */
  let registered = false;
  function register() {
    if (registered || !root) return; registered = true;
    const CE = root.CharmEditor || (root.CharmEditor = {});
    (CE.importers || (CE.importers = [])).push({ id: 'image', accept: ACCEPT.slice(), title: 'Image (PNG, JPG, WebP)', run });
    (CE.commands || (CE.commands = {})).imageRetrace = { title: 'Re-trace image…', group: 'Image', icon: IC.image, run: ctx => retrace(ctx), enabled: ctx => { try { const s = traceSet(ctx); return !!s && SOURCES.has(s.src); } catch (e) { return false; } } };
  }

  return { ACCEPT, DEF, BUDGET, norm, turn, flip, shownSize, keptSize, shownMatrix, prepGrey, greyImage, suggestLevels, levelsOf, grey, polysOf, inside, fitInOutline, fitInBox, target, sizeOf, fits, buildGroup, traceSet, greyName,
    register, run, retrace, decode, accepts, stats: () => stats, current: () => live, sources: () => SOURCES };
});
