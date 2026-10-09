/* Benchmark for the alignment of 10K and 14K sheets (see gold-block-align.cjs): one scenario per run, JSON on stdout.
 *   node tests/charm-nest/gold-block-bench.cjs <scenario> [--root DIR] [--orders N] [--seed N] [--batch N] [--fast] [--png PREFIX]
 * `--root` is a directory holding an older charm-nest-solver.js, charm-nest-rose.js and charm-nest-1.html (the BEFORE).
 * `--batch` is how many orders come in one update (a released batch of a slow metal: 13 by default; the old feed seated three at a
 * time). `--fast` turns the charms in 12° steps instead of 2° (about ten times quicker, for sweeps over seeds).
 * scenarios: full-100x50-14k  leftover-100x50-14k  full-50x46-14k  leftover-50x46-14k  rose-100x50 (the reference)
 * The straightness of a sheet's front is read on every row of the usable band: how far the farthest charm (or the cut-away stock)
 * reaches on that row. frontStd is the standard deviation of that reach in mm (0 = a straight front), maxFar/minFar its extremes,
 * envFill the share of the stock inside the next green line that the charms cover; fill is the sheet card's own figure.        */
'use strict';
const fs = require('node:fs'), zlib = require('node:zlib');
const L = require('./pocket-fill-lib.cjs');
const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? process.argv[i + 1] : d; };
const name = process.argv[2], root = arg('root', L.REPO), orders = +arg('orders', 26), seed = +arg('seed', 7), batch = +arg('batch', 13), png = arg('png', ''), fast = process.argv.includes('--fast');
const SC = {
  'full-100x50-14k': { metal: 'gold14k', w: 100, h: 50, grow: 1.35 },
  'leftover-100x50-14k': { metal: 'gold14k', w: 100, h: 50, left: true, grow: 1.35 },
  'full-50x46-14k': { metal: 'gold14k', w: 50, h: 46 },
  'leftover-50x46-14k': { metal: 'gold14k', w: 50, h: 46, left: true },
  'rose-100x50': { metal: 'rose', w: 100, h: 50, grow: 1.35 },
};
const sc = SC[name]; if (!sc) { console.error('scenario?', Object.keys(SC)); process.exit(2); }
const S = L.solverOf(root), wPt = L.PT(sc.w), hPt = L.PT(sc.h);
const stock = { wPt, hPt, ...(sc.left ? { remnant: L.leftover(wPt, hPt, sc.w === 50 ? L.SCREENSHOT_STEPS : [70, 60, 50, 40, 30, 22]) } : {}) };

function crc32(buf) { let c, crc = ~0; for (let n = 0; n < buf.length; n++) { c = (crc ^ buf[n]) & 0xff; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; crc = (crc >>> 8) ^ c; } return ~crc >>> 0; }
function writePng(file, w, h, rgb) {
  const raw = Buffer.alloc((w * 3 + 1) * h); for (let y = 0; y < h; y++) rgb.copy(raw, y * (w * 3 + 1) + 1, y * w * 3, (y + 1) * w * 3);
  const chunk = (t, d) => { const len = Buffer.alloc(4); len.writeUInt32BE(d.length); const td = Buffer.concat([Buffer.from(t), d]), c = Buffer.alloc(4); c.writeUInt32BE(crc32(td)); return Buffer.concat([len, td, c]); };
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 2;
  fs.writeFileSync(file, Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]));
}
/** The sheet as a picture: stock cut away in grey, each charm in its own colour, the front (the next green line's reach) in green. */
function render(file, page) {
  const g = L.gridOf(S, stock, [], [], -.5), Z = 2, w = g.W * Z, h = g.H * Z, buf = Buffer.alloc(w * h * 3, 255), PAL = [[214, 120, 90], [90, 150, 214], [120, 190, 110], [220, 190, 80], [170, 120, 200], [90, 190, 190], [230, 140, 170], [150, 150, 90]];
  const put = (x, y, c) => { for (let dy = 0; dy < Z; dy++) for (let dx = 0; dx < Z; dx++) { const i = ((y * Z + dy) * w + x * Z + dx) * 3; buf[i] = c[0]; buf[i + 1] = c[1]; buf[i + 2] = c[2]; } };
  for (let y = 0; y < g.H; y++) for (let x = 0; x < g.W; x++) if (g.get(x, y)) put(x, y, [225, 225, 225]);
  const byId = new Map(page.charms.map(c => [c.id, c]));
  page.placements.forEach((p, k) => {
    const v = S.prepareVariant(byId.get(p.id), p.angle, -.5, 2), x0 = Math.round(p.cxPt * 2 - v.solid.cx), y0 = Math.round(p.cyPt * 2 - v.solid.cy);
    for (let r = 0; r < v.fine.h; r++) for (let q = 0; q < v.fine.w; q++) if (v.fine.bits[r * v.fine.w + q]) { const x = x0 + q, y = y0 + r; if (x >= 0 && y >= 0 && x < g.W && y < g.H) put(x, y, PAL[k % PAL.length]); }
  });
  const env = L.envelopeOf(S, stock, page.placements, page.charms);
  for (let y = 0; y < env.FH; y++) put(Math.min(g.W - 1, env.far[y]), y, [0, 140, 120]);
  writePng(file, w, h, buf);
}
/** How straight the front of a sheet is, row by row over the usable band (1.5 pt inset = 3 cells). */
function front(page) {
  const env = L.envelopeOf(S, stock, page.placements, page.charms), far = [];
  for (let y = 3; y < env.FH - 3; y++) far.push(env.far[y] / 2 * L.MM);
  const mean = far.reduce((a, b) => a + b, 0) / far.length, std = Math.sqrt(far.reduce((a, b) => a + (b - mean) ** 2, 0) / far.length);
  return { frontStd: +std.toFixed(2), meanFar: +mean.toFixed(1), maxFar: +Math.max(...far).toFixed(1), minFar: +Math.min(...far).toFixed(1) };
}
(async () => {
  const stream = L.makeOrders(S, orders, seed, { small: .35, mid: .40, large: .25, two: .15, grow: sc.grow || 1 });
  const t0 = Date.now(), job = { timeBudgetMs: 36e5, stallMs: 36e5, ...(fast ? { angles: Array.from({ length: 30 }, (_, i) => i * 12) } : {}) };
  const run = await L.runStream({ root, metal: sc.metal, sheet: stock, orders: stream, batch, seed: 1, job });
  const m = L.measure(run, stock, { step: 2, cut: true });
  const rows = run.pages.filter(p => p.placements.length).map(p => {
    const mr = m.sheets.find(s => s.page === p.page) || {};
    if (png) render(`${png}-p${p.page}.png`, p);
    return { page: p.page, placed: p.placements.length, fill: mr.fill, envFill: mr.envFill, closed: mr.closed, ...front(p) };
  });
  const avg = k => +(rows.reduce((s, r) => s + (r[k] || 0), 0) / rows.length).toFixed(3);
  console.log(JSON.stringify({ scenario: name, root: root === L.REPO ? 'repo' : root, orders, batch, seed, fast, sheets: rows.length, placed: m.placedTotal, chargesPerSheet: +(m.placedTotal / rows.length).toFixed(2), meanFill: avg('fill'), meanEnvFill: avg('envFill'), meanFrontStd: avg('frontStd'), rows, wallMs: Date.now() - t0 }, null, 1));
})().catch(e => { console.error(e); process.exit(1); });
