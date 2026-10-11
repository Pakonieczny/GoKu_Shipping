/* Benchmark for the front of Gold-filled and Sterling Silver sheets (Paul, 11 Oct 2026): "I don't like how the GF and SS sheets
 * sometimes create these empty caverns. Please adapt both the GF and SS sheets to nest the pieces in a somewhat straight vertical
 * line front exactly like we already do with the RG 14/20 sheet."
 *   node tests/charm-nest/nest-front-bench.cjs <gold|silver|rose> [--root DIR] [--orders N] [--seed N] [--batches 2,3,5,4]
 *        [--fast] [--norun] [--save FILE] [--start FILE] [--fw GROWTH,ENV] [--ease FROM,TO,SCALE|0] [--step DEG] [--block 0|1] [--png PREFIX] [--snaps] [--grow F]
 * `--ease from,to,scale` replaces the easing of the block's growth price that Gold and Silver get (`0` for none: the full price, as Rose Gold pays),
 * `--fw g,e` replaces the block weights' growth (mm² per mm the block grows) and env (mm² per mm² of new space claimed).
 * `--root` is a directory holding an older charm-nest-solver.js, charm-nest-rose.js and charm-nest-1.html (the BEFORE); `--block 0`
 * runs the job as the page built it before this change (no block weights for Gold and Silver). `--batches` is how many orders
 * come in each update (the trickle of a day: 2 to 5; the sheet feeds them three at a time as the page does). `--fast` turns the
 * charms in 12° steps instead of 2°, `--step` in that many degrees. `--norun` is a sheet nested outside a run (a manual Nest): it
 * is not fed three orders at a time, the whole batch is placed in one search.
 * What it reads on every sheet (cells are 0.5 pt):
 *   frontStd   how straight the front is: the standard deviation, over the rows of the usable band, of how far the farthest charm
 *              reaches on that row (mm; 0 = a straight front)
 *   cavernMm   the widest empty disc (mm across) that fits behind the front: free space bounded by the charms, the sheet's edge band
 *              and the front itself (the 90th percentile of the rows' reach). 5 mm is a sliver; the screenshot's cavern is 13 mm
 *   blockFill  the share of the stock behind the front that the charms cover
 * `--snaps` also reads the first sheet after every update while it is 12 to 38% full (the screenshot was 21%). The density of a
 * stream is the fill of its closed sheets, the sheet card's own figure.                                                        */
'use strict';
const fs = require('node:fs'), zlib = require('node:zlib');
const L = require('./pocket-fill-lib.cjs');
const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? process.argv[i + 1] : d; };
const metal = process.argv[2] || 'gold', root = arg('root', L.REPO), orders = +arg('orders', 60), seed = +arg('seed', 1), png = arg('png', ''), fast = process.argv.includes('--fast'), snaps = process.argv.includes('--snaps');
const ease = arg('ease', ''), fw = arg('fw', ''), saveTo = arg('save', ''), startFrom = arg('start', ''), noRun = process.argv.includes('--norun'), step = +arg('step', fast ? 12 : 2), batches = arg('batches', '2,3,5,4').split(',').map(Number), blockArg = arg('block', ''), grow = +arg('grow', 1);
const S = L.solverOf(root), wPt = L.PT(100), hPt = L.PT(50), stock = { wPt, hPt };
const MMC = L.MM / 2;   // one fine cell in mm

function crc32(buf) { let c, crc = ~0; for (let n = 0; n < buf.length; n++) { c = (crc ^ buf[n]) & 0xff; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; crc = (crc >>> 8) ^ c; } return ~crc >>> 0; }
function pngBuf(w, h, rgb) {
  const raw = Buffer.alloc((w * 3 + 1) * h); for (let y = 0; y < h; y++) rgb.copy(raw, y * (w * 3 + 1) + 1, y * w * 3, (y + 1) * w * 3);
  const chunk = (t, d) => { const len = Buffer.alloc(4); len.writeUInt32BE(d.length); const td = Buffer.concat([Buffer.from(t), d]), c = Buffer.alloc(4); c.writeUInt32BE(crc32(td)); return Buffer.concat([len, td, c]); };
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 2;
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]);
}

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

/* ── a sheet as a picture (stock edge band in grey, each charm in its own colour, the front line in green) ───────── */
const PAL = [[214, 120, 90], [90, 150, 214], [120, 190, 110], [220, 190, 80], [170, 120, 200], [90, 190, 190], [230, 140, 170], [150, 150, 90]];
const FONT = { 'A': '01110 10001 10001 11111 10001 10001 10001', 'B': '11110 10001 11110 10001 10001 10001 11110', 'C': '01110 10001 10000 10000 10000 10001 01110', 'D': '11110 10001 10001 10001 10001 10001 11110', 'E': '11111 10000 11110 10000 10000 10000 11111', 'F': '11111 10000 11110 10000 10000 10000 10000', 'G': '01110 10001 10000 10111 10001 10001 01111', 'H': '10001 10001 11111 10001 10001 10001 10001', 'I': '01110 00100 00100 00100 00100 00100 01110', 'K': '10001 10010 11100 10010 10001 10001 10001', 'L': '10000 10000 10000 10000 10000 10000 11111', 'M': '10001 11011 10101 10101 10001 10001 10001', 'N': '10001 11001 10101 10011 10001 10001 10001', 'O': '01110 10001 10001 10001 10001 10001 01110', 'P': '11110 10001 11110 10000 10000 10000 10000', 'R': '11110 10001 11110 10100 10010 10001 10001', 'S': '01111 10000 01110 00001 00001 10001 01110', 'T': '11111 00100 00100 00100 00100 00100 00100', 'U': '10001 10001 10001 10001 10001 10001 01110', 'V': '10001 10001 10001 10001 01010 01010 00100', 'W': '10001 10001 10001 10101 10101 11011 10001', 'X': '10001 01010 00100 00100 00100 01010 10001', 'Y': '10001 01010 00100 00100 00100 00100 00100', '0': '01110 10011 10101 10101 11001 10001 01110', '1': '00100 01100 00100 00100 00100 00100 01110', '2': '01110 10001 00001 00110 01000 10000 11111', '3': '11110 00001 00110 00001 00001 10001 01110', '4': '00010 00110 01010 10010 11111 00010 00010', '5': '11111 10000 11110 00001 00001 10001 01110', '6': '01110 10000 11110 10001 10001 10001 01110', '7': '11111 00001 00010 00100 01000 01000 01000', '8': '01110 10001 01110 10001 10001 10001 01110', '9': '01110 10001 10001 01111 00001 00001 01110', ' ': '00000 00000 00000 00000 00000 00000 00000', '%': '11001 11010 00010 00100 01000 01011 10011', '.': '00000 00000 00000 00000 00000 01100 01100', ',': '00000 00000 00000 00000 01100 00100 01000', '/': '00001 00010 00010 00100 01000 01000 10000', ':': '00000 01100 01100 00000 01100 01100 00000', '-': '00000 00000 00000 11111 00000 00000 00000', '=': '00000 00000 11111 00000 11111 00000 00000' };
function sheetImage(page, Z) {
  const { FW, FH, owner, far } = cellsOf(page), w = FW * Z / 2 | 0, h = FH * Z / 2 | 0, buf = Buffer.alloc(w * h * 3, 250);
  const put = (x, y, c) => { if (x < 0 || y < 0 || x >= w || y >= h) return; const i = (y * w + x) * 3; buf[i] = c[0]; buf[i + 1] = c[1]; buf[i + 2] = c[2]; };
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { const fx = Math.floor(x * 2 / Z), fy = Math.floor(y * 2 / Z), k = owner[fy * FW + fx]; if (k >= 0) put(x, y, PAL[k % PAL.length]); else if (fx < 3 || fy < 3 || fx >= FW - 3 || fy >= FH - 3) put(x, y, [225, 225, 225]); }
  const f = front(page).frontP90 / MMC; for (let y = 0; y < h; y++) put(Math.round(f * Z / 2), y, [0, 140, 120]);
  return { w, h, buf };
}
function text(img, s, x0, y0, sc, col) { for (const ch of s.toUpperCase()) { const g = (FONT[ch] || FONT[' ']).split(' '); for (let r = 0; r < 7; r++) for (let c = 0; c < 5; c++) if (g[r][c] === '1') for (let dy = 0; dy < sc; dy++) for (let dx = 0; dx < sc; dx++) { const x = x0 + c * sc + dx, y = y0 + r * sc + dy; if (x >= 0 && y >= 0 && x < img.w && y < img.h) { const i = (y * img.w + x) * 3; img.buf[i] = col[0]; img.buf[i + 1] = col[1]; img.buf[i + 2] = col[2]; } } x0 += 6 * sc; } }
/** Pictures stacked into one PNG, each with a caption: [{ page, caption }] */
function composite(file, items, Z) {
  const imgs = items.map(it => ({ ...sheetImage(it.page, Z), caption: it.caption })), head = 16, pad = 6, W = Math.max(...imgs.map(i => i.w)) + 2 * pad, H = imgs.reduce((n, i) => n + i.h + head + pad, pad);
  const out = { w: W, h: H, buf: Buffer.alloc(W * H * 3, 255) };
  let y = pad;
  for (const im of imgs) {
    text(out, im.caption, pad, y + 2, 2, [40, 40, 40]); y += head;
    for (let r = 0; r < im.h; r++) im.buf.copy(out.buf, ((y + r) * W + pad) * 3, r * im.w * 3, (r + 1) * im.w * 3);
    for (let x = 0; x < im.w; x++) for (const yy of [y, y + im.h - 1]) { const i = (yy * W + pad + x) * 3; out.buf[i] = 120; out.buf[i + 1] = 120; out.buf[i + 2] = 120; }
    y += im.h + pad;
  }
  fs.writeFileSync(file, pngBuf(W, H, out.buf));
}

async function run() {
  const stream = L.makeOrders(S, orders, seed, { small: .35, mid: .40, large: .25, two: .15, grow });
  // (every angle of the circle at `step` degrees: 180 of them at the page's 2°)
  const t0 = Date.now(), job = { timeBudgetMs: 36e5, stallMs: 36e5, angles: Array.from({ length: Math.round(360 / step) }, (_, i) => i * step), ...(fw ? { fitWeights: { growth: +fw.split(',')[0], env: +fw.split(',')[1] } } : {}), ...(ease ? { blockEase: ease === '0' ? null : { from: +ease.split(',')[0], to: +ease.split(',')[1], scale: +ease.split(',')[2] } } : {}) };
  const snapsOut = [];
  const progress = (n, total, ctl) => {
    if (!snaps) return;
    const p = ctl.pages[0]; if (!p || !p.placements.length || !p.usablePt2) return;
    const fill = 1 - p.freePt2 / p.usablePt2;
    if (fill >= .12 && fill <= .38) snapsOut.push({ after: n, placed: p.placements.length, fill: +fill.toFixed(3), ...front(p), page: { charms: p.charms.slice(), placements: p.placements.map(q => ({ ...q })) } });
  };
  let start = null, rest = stream;
  if (startFrom) {   // a saved sheet (written by --save): the orders after it are appended to it
    const f = JSON.parse(fs.readFileSync(startFrom, 'utf8')), byOrder = new Map(stream.map(o => [o.id, o]));
    start = { charms: f.orders.flatMap(id => byOrder.get(id).pieces), placements: f.placements };
    const done = new Set(f.orders); rest = stream.filter(o => !done.has(o.id));
  }
  const run = await L.runStream({ root, ...(start ? { start } : {}), metal, sheet: stock, orders: rest, batch: batches, seed: 1, job, progress, ...(noRun ? { runId: null } : {}), ...(blockArg !== '' ? { block: blockArg === '1' } : {}) });
  const rows = run.pages.filter(p => p.placements.length).map(p => {
    const fill = p.usablePt2 ? 1 - p.freePt2 / p.usablePt2 : p.density;
    if (png) fs.writeFileSync(`${png}-p${p.page}.png`, (() => { const im = sheetImage(p, 4); return pngBuf(im.w, im.h, im.buf); })());
    return { page: p.page, placed: p.placements.length, fill: +fill.toFixed(3), closed: run.closed(p), ...front(p) };
  });
  if (saveTo && snapsOut.length) {   // the snapshot with the widest cavern: the saved sheet a later run appends to
    const w = snapsOut.reduce((a, b) => b.cavernMm > a.cavernMm ? b : a), ids = new Set(w.page.placements.map(p => p.id));
    fs.writeFileSync(saveTo, JSON.stringify({ seed, grow, cavernMm: w.cavernMm, fill: w.fill, orders: stream.filter(o => o.pieces.every(c => ids.has(c.id))).map(o => o.id), placements: w.page.placements }));
  }
  const closed = rows.filter(r => r.closed), avg = (a, k) => a.length ? +(a.reduce((s, r) => s + (r[k] || 0), 0) / a.length).toFixed(3) : null;
  const sn = snaps ? { n: snapsOut.length, frontStd: avg(snapsOut, 'frontStd'), cavernMm: avg(snapsOut, 'cavernMm'), blockFill: avg(snapsOut, 'blockFill'), maxCavernMm: Math.max(0, ...snapsOut.map(s => s.cavernMm)) } : undefined;
  const res = { metal, root: root === L.REPO ? 'repo' : root, orders, seed, batches: batches.join(','), step, block: blockArg, sheets: rows.length, closedSheets: closed.length, placed: rows.reduce((n, r) => n + r.placed, 0),
    density: avg(closed, 'fill'), densityAll: avg(rows, 'fill'), closedFrontStd: avg(closed, 'frontStd'), closedCavernMm: avg(closed, 'cavernMm'), closedBlockFill: avg(closed, 'blockFill'), maxClosedCavernMm: Math.max(0, ...closed.map(r => r.cavernMm)), snaps: sn, rows, wallS: Math.round((Date.now() - t0) / 1000) };
  return { res, snapsOut, run };
}
module.exports = { front, composite, sheetImage, run };
if (require.main === module) run().then(({ res }) => console.log(JSON.stringify(res, null, 1))).catch(e => { console.error(e); process.exit(1); });
