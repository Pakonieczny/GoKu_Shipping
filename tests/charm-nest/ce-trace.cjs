// IMAGE TRACE (charm-nest-ce-trace.js; Paul, 28 Oct 2026: "All images must be converted to a vector format while keeping the quality of the image and the colour
// should be a greyscale using the black as base"; the lead's D2). No server, no network, no live service: synthetic pictures drawn here, and the repo's own photo.
//   node tests/charm-nest/ce-trace.cjs                       (the node checks; the browser check is skipped without playwright-core)
//   $SP/int/slot.sh node tests/charm-nest/ce-trace.cjs       (with the browser check of the Worker path)
// Quality is measured by drawing the result back into pixels (non-zero rule, at the pixel centres) and comparing it with the posterised masks it was traced from:
// IoU = |both| / |either|, per band (what is engraved at that grey) and per nested mask M_k (that grey or darker).
// 1 circles in four greys (an illustration): IoU per band and per mask > 0.97; the bands share their borders (supersampled 4 × 4: the pairwise overlap is ~0 and
//   their areas add up to M_(N-1)'s within 0.5 %); a lone disc in a few Béziers; greys 0, .25, .5, .75; suggestLevels 4
// 2 block letters (line art, 1 level): IoU > 0.97, the square corners kept as corners, suggestLevels 1
// 3 a gradient at 4 levels: four strips, a handful of nodes each, the Otsu cuts at even steps
// 4 a 1-megapixel photo at 4 levels: the time (target under 1.5 s), IoU per mask and per band, nodes; suggestLevels 5
// 5 despeckle in mm² on the charm; smooth 0 = a polygon, smooth 1 = no corners; 6 grey(): black is the base, invert, transparent = white; posterise() is quick
// 7 trace() without a Worker: on this thread in slices under 50 ms, progress to 1, the same result as traceSync, cancel() stops it
// 8 the browser: the Worker made from a Blob URL of the file, progress, no long task on the page while a photo is traced, the same result as on the page, cancel
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const T = require(path.join(root, 'charm-nest-ce-trace.js'));
const results = [], failures = [], numbers = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n         ')); };
async function step(name, fn) { try { await fn(); pass(name); } catch (e) { fail(name, e); } }
const say = s => { numbers.push(s); console.log('       ' + s); };
const f3 = v => (Math.round(v * 1000) / 1000).toFixed(3);

/* ── pictures ── */
/** A grey picture from a coverage function: g(x, y) -> grey 0..1 at a point; antialiased by 4 × 4 samples, as a scanned or exported picture is. */
function picture(W, H, g, ss = 4) {
  const data = new Uint8ClampedArray(W * H * 4);
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
    let s = 0; for (let j = 0; j < ss; j++) for (let i = 0; i < ss; i++) s += g(x + (i + 0.5) / ss, y + (j + 0.5) / ss);
    const v = Math.round(255 * s / (ss * ss)), k = (y * W + x) * 4; data[k] = data[k + 1] = data[k + 2] = v; data[k + 3] = 255;
  }
  return { data, width: W, height: H };
}
const inDisc = (x, y, cx, cy, r) => (x - cx) ** 2 + (y - cy) ** 2 < r * r;
// four tones and white; overlapping discs, an annulus and a disc inside a ring
const CIRCLES = (x, y) => {
  if (inDisc(x, y, 230, 230, 150)) return inDisc(x, y, 230, 230, 60) ? 0.8 : 0;
  if (inDisc(x, y, 600, 260, 130)) return inDisc(x, y, 540, 260, 50) ? 0 : 0.3;
  if (inDisc(x, y, 470, 520, 150) && !inDisc(x, y, 470, 520, 80)) return 0.55;
  if (inDisc(x, y, 470, 520, 45)) return 0;
  if (inDisc(x, y, 790, 560, 90)) return 0.8;
  return 1;
};
/** Block capitals C H A R M 3 7 as rectangles and round counters: square corners and round ones. */
function letters(x, y) {
  const R = (x0, y0, x1, y1) => x >= x0 && x < x1 && y >= y0 && y < y1;
  const s = 30, top = 60, bot = 240;
  const ink =
    // C: a ring cut open on the right
    (inDisc(x, y, 130, 150, 90) && !inDisc(x, y, 130, 150, 90 - s) && !(x > 150 && y > 110 && y < 190)) ||
    // H
    R(250, top, 250 + s, bot) || R(340, top, 340 + s, bot) || R(250, 135, 370, 165) ||
    // A (square): two legs, top, bar
    R(420, top, 420 + s, bot) || R(510, top, 510 + s, bot) || R(420, top, 540, top + s) || R(420, 140, 540, 170) ||
    // R: leg, bowl (ring top-right), leg
    R(590, top, 590 + s, bot) || (x >= 590 && inDisc(x, y, 650, 115, 55) && !inDisc(x, y, 650, 115, 25)) || R(650, 160, 650 + s, bot) || R(590, top, 650, top + s) ||
    // M
    R(740, top, 740 + s, bot) || R(830, top, 830 + s, bot) || R(785, top, 785 + 20, 170) || R(740, top, 860, top + s) ||
    // 3 and 7
    R(910, top, 1000, top + s) || R(910, 135, 1000, 165) || R(910, bot - s, 1000, bot) || R(970, top, 1000, bot) ||
    R(1040, top, 1140, top + s) || (y >= top && y < bot && Math.abs((x - 1060) - (bot - y) * 0.45) < 16);
  return ink ? 0 : 1;
}
/** A photo: the repo's own (assets/Michelle_R.jpg, read with sharp when it is installed), else a synthetic one of noise, shapes and a gradient. */
async function photo(W, H) {
  try {
    const sharp = require(path.join(root, 'node_modules', 'sharp'));
    const { data, info } = await sharp(path.join(root, 'assets', 'Michelle_R.jpg')).resize(W, H, { fit: 'fill' }).ensureAlpha().raw().toBuffer({ resolveWithObject: true });
    return { img: { data: new Uint8ClampedArray(data.buffer, data.byteOffset, data.length), width: info.width, height: info.height }, what: 'assets/Michelle_R.jpg' };
  } catch (_) {
    let seed = 7; const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
    const G = 64, grid = Array.from({ length: (G + 1) * (G + 1) }, rnd), noise = (x, y) => {
      const gx = x / W * G, gy = y / H * G, i = Math.floor(gx), j = Math.floor(gy), u = gx - i, v = gy - j, a = grid[j * (G + 1) + i], b = grid[j * (G + 1) + i + 1], c = grid[(j + 1) * (G + 1) + i], d = grid[(j + 1) * (G + 1) + i + 1];
      return (a * (1 - u) + b * u) * (1 - v) + (c * (1 - u) + d * u) * v;
    };
    const img = picture(W, H, (x, y) => Math.max(0, Math.min(1, 0.5 * noise(x, y) + 0.3 * x / W + (inDisc(x, y, W * 0.4, H * 0.5, H * 0.3) ? -0.3 : 0.1) + 0.08 * (rnd() - 0.5))), 1);
    return { img, what: 'synthetic (sharp not installed)' };
  }
}

/* ── drawing the result back ── */
function flatten(sub, k) {
  const pts = []; let cur = null;
  for (const op of sub) {
    if (op[0] === 'm' || op[0] === 'l') { cur = [op[1][0] * k, op[1][1] * k]; pts.push(cur); }
    else if (op[0] === 'c') {
      const p1 = [op[1][0] * k, op[1][1] * k], p2 = [op[2][0] * k, op[2][1] * k], p3 = [op[3][0] * k, op[3][1] * k];
      const len = Math.hypot(p1[0] - cur[0], p1[1] - cur[1]) + Math.hypot(p2[0] - p1[0], p2[1] - p1[1]) + Math.hypot(p3[0] - p2[0], p3[1] - p2[1]);
      const n = Math.max(4, Math.ceil(len / 0.4));
      for (let i = 1; i <= n; i++) {
        const t = i / n, u = 1 - t;
        pts.push([u * u * u * cur[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t * t * t * p3[0], u * u * u * cur[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t * t * t * p3[1]]);
      }
      cur = p3;
    }
  }
  return pts;
}
/** Subpaths (source px) to samples: ss × ss per pixel, filled by the non-zero rule at each sample's centre. */
function raster(subpaths, W, H, ss = 1) {
  const WS = W * ss, HS = H * ss, cross = Array.from({ length: HS }, () => []);
  for (const sub of subpaths) {
    const p = flatten(sub, ss);
    for (let i = 0; i < p.length; i++) {
      const a = p[i], b = p[(i + 1) % p.length];
      if (a[1] === b[1]) continue;
      const dir = b[1] > a[1] ? 1 : -1, ya = Math.min(a[1], b[1]), yb = Math.max(a[1], b[1]);
      for (let r = Math.max(0, Math.ceil(ya - 0.5)); r <= Math.min(HS - 1, Math.ceil(yb - 0.5) - 1); r++) {
        const yc = r + 0.5; cross[r].push({ x: a[0] + (b[0] - a[0]) * (yc - a[1]) / (b[1] - a[1]), d: dir });
      }
    }
  }
  const out = new Uint8Array(WS * HS);
  for (let r = 0; r < HS; r++) {
    const c = cross[r].sort((u, v) => u.x - v.x); let wind = 0;
    for (let i = 0; i < c.length - 1; i++) {
      wind += c[i].d;
      if (wind) for (let x = Math.max(0, Math.ceil(c[i].x - 0.5)); x <= Math.min(WS - 1, Math.ceil(c[i + 1].x - 0.5) - 1); x++) out[r * WS + x] = 1;
    }
  }
  return out;
}
function iou(a, b) { let both = 0, either = 0; for (let i = 0; i < a.length; i++) { if (a[i] && b[i]) both++; if (a[i] || b[i]) either++; } return either ? both / either : 1; }
const maskOf = (labels, test) => { const m = new Uint8Array(labels.length); for (let i = 0; i < labels.length; i++) m[i] = test(labels[i]) ? 1 : 0; return m; };
/** IoU of each band and each nested mask against the labels the trace worked from (scale 1 only). */
function quality(res, labels) {
  const W = res.work.width, H = res.work.height, N = res.levels, band = [], cum = [];
  const drawn = Array.from({ length: N }, (_, k) => { const L = res.layers.find(l => l.level === k); return L ? raster(L.subpaths, W, H) : new Uint8Array(W * H); });
  let acc = new Uint8Array(W * H);
  for (let k = 0; k < N; k++) {
    band.push(iou(drawn[k], maskOf(labels, l => l === k)));
    acc = acc.map((v, i) => v | drawn[k][i]);
    cum.push(iou(acc, maskOf(labels, l => l <= k)));
  }
  return { band, cum };
}
/** How many borders a mask has: its shapes (dark pixels touching at a corner are one, as the trace keeps them) and their holes (white touching by a side). */
function borders(labels, W, H) {
  const seen = new Uint8Array(W * H); let n = 0;
  const fill = (s, ink) => { const st = [s]; seen[s] = 1; while (st.length) { const p = st.pop(), x = p % W, y = (p - x) / W;
    for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) { if (!dx && !dy || (!ink && dx && dy)) continue; const xx = x + dx, yy = y + dy;
      if (xx < 0 || yy < 0 || xx >= W || yy >= H) continue; const q = yy * W + xx; if (!seen[q] && (labels[q] === 0) === ink) { seen[q] = 1; st.push(q); } } } };
  // the white touching the picture's edge is the outside, not a hole
  for (let x = 0; x < W; x++) for (const y of [0, H - 1]) if (!seen[y * W + x] && labels[y * W + x] !== 0) fill(y * W + x, false);
  for (let y = 0; y < H; y++) for (const x of [0, W - 1]) if (!seen[y * W + x] && labels[y * W + x] !== 0) fill(y * W + x, false);
  for (let i = 0; i < W * H; i++) if (!seen[i]) { fill(i, labels[i] === 0); n++; }
  return n;
}
const ops = res => { const c = { m: 0, l: 0, c: 0, h: 0 }; for (const L of res.layers) for (const s of L.subpaths) for (const o of s) c[o[0]]++; return c; };
function wellFormed(res) {
  for (const L of res.layers) for (const s of L.subpaths) {
    assert.equal(s[0][0], 'm'); assert.equal(s[s.length - 1][0], 'h');
    for (const o of s) for (const p of o.slice(1)) assert.ok(Array.isArray(p) && p.length === 2 && Number.isFinite(p[0]) && Number.isFinite(p[1]) && p[0] >= 0 && p[1] >= 0 && p[0] <= res.width && p[1] <= res.height, 'every point, control points too, inside the picture: ' + JSON.stringify(o));
  }
}

(async () => {
  /* ════ 1 · circles in four greys ════ */
  await step('circles in four greys: IoU per band and per nested mask > 0.97; bands share borders (no overlap, areas add up); greys 0 .25 .5 .75', () => {
    const img = picture(900, 700, CIRCLES);
    const res = T.traceSync(img, { levels: 4, keepLabels: true });
    wellFormed(res);
    assert.deepEqual(res.layers.map(L => L.grey), [0, 0.25, 0.5, 0.75]);
    assert.equal(res.suggestLevels, 4, 'an illustration: ' + res.content);
    const q = quality(res, res.work.labels), qr = quality(res, res.work.labelsRaw);
    say(`circles 900×700 4 levels: ${f3(res.ms.total)} ms, thresholds ${res.thresholds.join(' ')}, nodes ${res.layers.map(L => L.nodes).join(' / ')}`);
    say(`  IoU per band ${q.band.map(f3).join(' ')} · per mask ${q.cum.map(f3).join(' ')} · (before despeckle: bands ${qr.band.map(f3).join(' ')})`);
    for (const v of q.band.concat(q.cum)) assert.ok(v > 0.97, 'IoU ' + v);
    // supersampled 4 × 4: what lies under two bands, and the bands' total against the darkest-to-lightest mask traced on its own (stack mode)
    const ss = 4, W = 900, H = 700, drawn = res.layers.map(L => raster(L.subpaths, W, H, ss));
    const stacked = T.traceSync(img, { levels: 4, stack: true }), all = raster(stacked.layers[stacked.layers.length - 1].subpaths, W, H, ss);
    let sum = 0, over = 0, union = 0, allN = 0; const perPx = new Uint8Array(W * H);
    for (let i = 0; i < all.length; i++) {
      let c = 0; for (const d of drawn) c += d[i]; sum += c; if (c > 1) { over += c - 1; perPx[Math.floor(i / (W * ss) / ss) * W + Math.floor((i % (W * ss)) / ss)]++; } if (c) union++; allN += all[i];
    }
    let widest = 0; for (const c of perPx) if (c > widest) widest = c; widest /= ss * ss;
    say(`  bands supersampled 4×4: overlap ${(100 * over / sum).toFixed(3)} % of the ink (slivers: at most ${widest.toFixed(2)} of a pixel), Σ bands / M_3 = ${(sum / allN).toFixed(5)}, union / M_3 = ${(union / allN).toFixed(5)}`);
    assert.ok(over / sum < 0.0025 && widest <= 0.5, 'bands overlap: ' + over + ' samples, ' + widest + ' px');
    assert.ok(Math.abs(sum / allN - 1) < 0.005, 'bands add up to M_3 within 0.5 %: ' + sum / allN);
    // a lone disc (r 90, grey .8) in few Béziers
    const lone = T.traceSync(picture(240, 240, (x, y) => (inDisc(x, y, 120, 120, 90) ? 0.8 : 1)), { levels: 1 });
    say(`  a lone disc r 90: ${lone.layers[0].nodes} nodes (${ops(lone).c} Béziers, ${ops(lone).l} lines)`);
    assert.ok(lone.layers[0].nodes <= 12 && ops(lone).l === 0, 'a disc in a few curves');
  });

  /* ════ 2 · block letters ════ */
  await step('block letters at 1 level: IoU > 0.97, square corners kept, suggestLevels 1', () => {
    const img = picture(1200, 300, letters);
    const res = T.traceSync(img, { levels: 1, keepLabels: true });
    wellFormed(res);
    const q = quality(res, res.work.labels), c = ops(res);
    say(`letters 1200×300: ${f3(res.ms.total)} ms, IoU ${f3(q.band[0])}, ${res.layers[0].subpaths.length} subpaths, ${res.nodes} nodes (${c.l} lines, ${c.c} Béziers)`);
    assert.ok(q.band[0] > 0.97, 'IoU ' + q.band[0]);
    assert.equal(res.suggestLevels, 1); assert.equal(res.content, 'line');
    // the H's top-left corner (250, 60) is a node, not a rounded curve
    const nearCorner = res.layers[0].subpaths.some(s => s.some(o => o[0] !== 'h' && Math.hypot(o[o.length - 1][0] - 250, o[o.length - 1][1] - 60) < 0.8));
    assert.ok(nearCorner, 'a node within 0.8 px of the H corner');
    assert.ok(c.l >= 40, 'corners kept: ' + c.l + ' line ends');
    assert.equal(res.layers[0].subpaths.length, borders(res.work.labels, res.work.width, res.work.height), 'one subpath per border: every shape and every counter');
  });

  /* ════ 3 · gradient ════ */
  await step('a gradient at 4 levels: four strips, few nodes, Otsu cuts at even steps', () => {
    const img = picture(1024, 256, x => x / 1024, 1);
    const res = T.traceSync(img, { levels: 4, keepLabels: true });
    const q = quality(res, res.work.labels);
    say(`gradient 1024×256: thresholds ${res.thresholds.join(' ')}, nodes ${res.layers.map(L => L.nodes).join(' / ')}, IoU bands ${q.band.map(f3).join(' ')}`);
    assert.equal(res.layers.length, 4);
    for (const L of res.layers) assert.ok(L.nodes <= 8, 'a strip in few nodes: ' + L.nodes);
    for (const v of q.band) assert.ok(v > 0.99);
    res.thresholds.forEach((t, k) => assert.ok(Math.abs(t - (k + 1) / 5) < 0.02, 'even cut ' + t));
  });

  /* ════ 4 · a photo, 1 megapixel ════ */
  let PHOTO = null;
  await step('a 1-megapixel photo at 4 levels: time (target < 1.5 s), IoU per mask, nodes', async () => {
    const { img, what } = await photo(1414, 707); PHOTO = img;
    T.traceSync(img, { levels: 4 });                       // the first run warms the JIT; the second is the number
    const res = T.traceSync(img, { levels: 4, keepLabels: true });
    const q = quality(res, res.work.labels), qr = quality(res, res.work.labelsRaw);
    say(`photo ${img.width}×${img.height} (${what}): ${res.ms.total.toFixed(0)} ms (prep ${res.ms.prep.toFixed(0)}, posterise ${res.ms.posterise.toFixed(0)}, trace ${res.ms.trace.toFixed(0)}, unpack ${(res.ms.total - res.ms.work).toFixed(0)}), ${res.content}, suggest ${res.suggestLevels}`);
    say(`  thresholds ${res.thresholds.join(' ')}, specks merged ${res.work.merged} (≤ ${res.work.speckPx} px), nodes ${res.layers.map(L => L.nodes).join(' / ')} = ${res.nodes}`);
    say(`  IoU per mask ${q.cum.map(f3).join(' ')} · per band ${q.band.map(f3).join(' ')} · per mask before despeckle ${qr.cum.map(f3).join(' ')}`);
    assert.ok(res.ms.total < 2500, 'a 1 MP photo traced in ' + res.ms.total.toFixed(0) + ' ms (target 1500; 2500 allowed on a loaded machine)');
    for (const v of q.cum) assert.ok(v > 0.97, 'IoU per mask ' + v);
    for (const v of q.band) assert.ok(v > 0.93, 'IoU per band ' + v);
    assert.equal(res.suggestLevels, 5); assert.equal(res.content, 'photo');
    wellFormed(res);
  });

  /* ════ 5 · despeckle and smoothness ════ */
  await step('despeckle in mm² on the charm; smooth 0 = a polygon, smooth 1 = no corners', () => {
    let seed = 3; const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
    const W = 400, H = 300, data = new Uint8ClampedArray(W * H * 4).fill(255);
    const ink = (x, y) => { const k = (y * W + x) * 4; data[k] = data[k + 1] = data[k + 2] = 0; };
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) if (inDisc(x + 0.5, y + 0.5, 200, 150, 70)) ink(x, y);
    for (let i = 0; i < 60; i++) { const x = 5 + Math.floor(rnd() * 380), y = 5 + Math.floor(rnd() * 280); if (inDisc(x, y, 200, 150, 80)) continue; ink(x, y); if (i % 2) ink(x + 1, y); if (i % 3 === 0) ink(x, y + 1); }
    const img = { data, width: W, height: H };
    const none = T.traceSync(img, { levels: 1, despeckle: 0 }), dflt = T.traceSync(img, { levels: 1 }), big = T.traceSync(img, { levels: 1, despeckleMm2: 0.02 });
    const at10 = T.traceSync(img, { levels: 1, despeckleMm2: 0.02, pxPerMm: 10 });
    const count = r => r.layers[0].subpaths.length;
    const at10b = T.traceSync(img, { levels: 1, despeckleMm2: 0.04, pxPerMm: 10 });
    say(`specks: despeckle 0 → ${count(none)} shapes; default 2 px at 20 px/mm (0.005 mm²) → ${count(dflt)}; 0.02 mm² (8 px) → ${count(big)}; the picture at 10 px/mm (each pixel 0.01 mm²): 0.02 mm² → ${count(at10)}, 0.04 mm² → ${count(at10b)}`);
    assert.ok(count(none) >= 45 && count(dflt) > 1 && count(dflt) < count(none) && count(big) === 1, 'specks go, the disc stays');
    assert.ok(count(at10) > 1 && count(at10b) === 1, 'measured on the charm: the same 3-pixel speck is 0.03 mm² at 10 px/mm');
    const sq = picture(200, 200, (x, y) => (x > 40 && x < 160 && y > 40 && y < 160) || inDisc(x, y, 100, 100, 0) ? 0 : 1);
    const poly = T.traceSync(sq, { levels: 1, smooth: 0 }), round = T.traceSync(sq, { levels: 1, smooth: 1 }), mid = T.traceSync(sq, { levels: 1 });
    say(`a square: smooth 0 → ${ops(poly).l} lines ${ops(poly).c} curves; 0.5 → ${ops(mid).l} lines ${ops(mid).c} curves; 1 → ${ops(round).l} lines ${ops(round).c} curves`);
    assert.equal(ops(poly).c, 0); assert.equal(ops(round).l, 0); assert.equal(ops(mid).c, 0, 'a square at the default smoothness is four corners');
  });

  /* ════ 6 · grey() and posterise() ════ */
  await step('grey(): black is the base (auto-levels), invert, transparent is white; posterise() small and quick', () => {
    const W = 64, H = 32, data = new Uint8ClampedArray(W * H * 4);
    for (let i = 0; i < W * H; i++) { const v = 100 + Math.round(100 * (i % W) / (W - 1)); data[i * 4] = v; data[i * 4 + 1] = v; data[i * 4 + 2] = v; data[i * 4 + 3] = 255; }
    const g = T.grey({ data, width: W, height: H }), gi = T.grey({ data, width: W, height: H }, { invert: true });
    let lo = 255, hi = 0; for (let i = 0; i < W * H; i++) { lo = Math.min(lo, g.data[i * 4]); hi = Math.max(hi, g.data[i * 4]); }
    for (let i = 0; i < W; i++) data[i * 4 + 3] = 0;
    const gt = T.grey({ data, width: W, height: H });
    say(`grey of a 100..200 picture: ${lo}..${hi}; a transparent row → ${gt.data[0]}; inverted first pixel ${gi.data[0]} (was ${g.data[0]})`);
    assert.equal(lo, 0, 'the darkest is black'); assert.equal(hi, 255); assert.equal(gt.data[0], 255, 'transparent is white');
    assert.equal(gi.data[0], 255 - g.data[0]);
    const img = picture(900, 700, CIRCLES), t = Date.now(), p = T.posterise(img, { levels: 4 }), took = Date.now() - t;
    say(`posterise preview: ${p.width}×${p.height} in ${took} ms, thresholds ${p.thresholds.join(' ')}, suggest ${p.suggestLevels}`);
    assert.ok(Math.max(p.width, p.height) === 512 && took < 150);
    const s = T.suggest(img); assert.equal(s.levels, 4);
  });

  /* ════ 7 · trace() without a Worker ════ */
  await step('trace() with no Worker: on this thread in slices under 50 ms, progress to 1, the same as traceSync; cancel() stops it', async () => {
    assert.equal(typeof Worker, 'undefined');
    const img = PHOTO || picture(900, 700, CIRCLES), seen = [];
    const res = await T.trace(img, { levels: 4, onProgress: p => seen.push(p) });
    const sync = T.traceSync(img, { levels: 4 });
    say(`on this thread: ${res.ms.total.toFixed(0)} ms, longest slice ${res.ms.longestSliceOnPage} ms, ${seen.length} progress calls, last ${seen[seen.length - 1]}`);
    assert.equal(res.worker, false);
    assert.ok(res.ms.longestSliceOnPage < 50, 'longest slice ' + res.ms.longestSliceOnPage);
    assert.equal(seen[seen.length - 1], 1); assert.ok(seen.every((p, i) => !i || p >= seen[i - 1]), 'progress only goes up');
    assert.deepEqual(res.layers.map(L => L.nodes), sync.layers.map(L => L.nodes));
    assert.deepEqual(res.layers[0].subpaths.slice(0, 3), sync.layers[0].subpaths.slice(0, 3));
    let p0 = 0; const pr = T.trace(img, { levels: 4, onProgress: p => { p0 = p; } });
    await new Promise(r => setTimeout(r, 60));
    assert.equal(T.cancel(), 1);
    await assert.rejects(pr, e => e.name === 'AbortError' && e.cancelled === true);
    say(`  cancelled at ${(100 * p0).toFixed(0)} %`);
    const ac = new AbortController(), pa = T.trace(img, { levels: 2, signal: ac.signal }); setTimeout(() => ac.abort(), 20);
    await assert.rejects(pa, e => e.name === 'AbortError');
    assert.equal(T.busy, 0);
  });

  await step('served: a script tag with a cache token in charm-nest-1.html, and in the public manifest', () => {
    assert.match(fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), /<script src="charm-nest-ce-trace\.js\?v=[^"]+"><\/script>/);
    assert.ok(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8').includes('"charm-nest-ce-trace.js"'));
  });

  /* ════ 8 · the browser: the Worker path ════ */
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium = null; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser check was not run'); }
  if (chromium) await step('browser: the Worker from a Blob URL, progress, no long task while a photo is traced, the same result as the page, cancel', async () => {
    const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
    try {
      const ctx = await browser.newContext();
      const files = { '/': ['text/html', '<!doctype html><meta charset="utf-8"><body><script src="charm-nest-ce-trace.js"></script>'],
        '/charm-nest-ce-trace.js': ['text/javascript', fs.readFileSync(path.join(root, 'charm-nest-ce-trace.js'))],
        '/photo.jpg': ['image/jpeg', fs.readFileSync(path.join(root, 'assets', 'Michelle_R.jpg'))] };
      await ctx.route('**/*', r => { const u = new URL(r.request().url()); const f = u.origin === 'https://trace.test' && files[u.pathname]; return f ? r.fulfill({ contentType: f[0], body: f[1] }) : r.abort(); });
      const page = await ctx.newPage(); const errs = [];
      page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });
      await page.goto('https://trace.test/');
      const out = await page.evaluate(async () => {
        const T = window.CharmNestTrace, long = [];
        new PerformanceObserver(l => { for (const e of l.getEntries()) long.push(Math.round(e.duration)); }).observe({ type: 'longtask', buffered: false });
        const bmp = await createImageBitmap(await (await fetch('/photo.jpg')).blob(), { resizeWidth: 1414, resizeHeight: 707, resizeQuality: 'high' });
        const cv = document.createElement('canvas'); cv.width = 1414; cv.height = 707; const cx = cv.getContext('2d'); cx.drawImage(bmp, 0, 0);
        const img = cx.getImageData(0, 0, 1414, 707);
        await new Promise(r => setTimeout(r, 100)); long.length = 0;
        // frames while the photo is traced: the page keeps painting
        let frames = 0, gap = 0, last = performance.now(), on = true;
        const tick = () => { if (!on) return; const t = performance.now(); gap = Math.max(gap, t - last); last = t; frames++; requestAnimationFrame(tick); }; requestAnimationFrame(tick);
        const prog = [];
        const res = await T.trace(img, { levels: 4, onProgress: p => prog.push(p) });
        on = false;
        const longDuring = long.slice();
        const sync = T.traceSync(img, { levels: 4 });
        // cancel a running trace, then trace again (a fresh Worker)
        const pc = T.trace(img, { levels: 4 }); await new Promise(r => setTimeout(r, 80));
        const n = T.cancel(); let why = null; try { await pc; } catch (e) { why = e.name; }
        const again = await T.trace(img, { levels: 2 });
        return { worker: res.worker, ms: Math.round(res.ms.total), work: Math.round(res.ms.work), longestSlice: res.ms.longestSliceOnPage, nodes: res.layers.map(L => L.nodes), syncNodes: sync.layers.map(L => L.nodes),
          syncMs: Math.round(sync.ms.total), first: JSON.stringify(res.layers[0].subpaths[0]).slice(0, 120), syncFirst: JSON.stringify(sync.layers[0].subpaths[0]).slice(0, 120),
          prog: prog.length, lastProg: prog[prog.length - 1], longDuring, frames, gap: Math.round(gap), cancelled: n, why, againWorker: again.worker, againLayers: again.layers.length };
      });
      say(`browser Worker: photo 1414×707 4 levels in ${out.ms} ms (work in the Worker ${out.work} ms; on the page ${out.syncMs} ms), unpack slices ≤ ${out.longestSlice} ms, ${out.prog} progress calls`);
      say(`  while it ran: ${out.frames} frames, longest gap ${out.gap} ms, long tasks ${JSON.stringify(out.longDuring)}; nodes ${out.nodes.join(' / ')}; cancel → ${out.why}, again in a fresh Worker: ${out.againWorker}`);
      assert.equal(out.worker, true, 'the Worker did it');
      assert.deepEqual(out.nodes, out.syncNodes, 'the same result as on the page'); assert.equal(out.first, out.syncFirst);
      assert.ok(out.prog >= 3 && out.lastProg === 1, 'progress');
      assert.deepEqual(out.longDuring, [], 'no long task on the page while the Worker traces');
      assert.equal(out.cancelled, 1); assert.equal(out.why, 'AbortError'); assert.equal(out.againWorker, true); assert.equal(out.againLayers, 2);
      assert.deepEqual(errs, []);
    } finally { await browser.close(); }
  });

  console.log(`\n${results.length} passed, ${failures.length} failed`);
  if (failures.length) { console.log(failures.map(f => '  - ' + f).join('\n')); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
